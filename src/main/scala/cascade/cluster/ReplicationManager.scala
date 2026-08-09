package cascade.cluster

import cascade.broker.BrokerConfig
import cascade.protocol.{ByteCursor, ByteWriter, Errors}
import cascade.storage.TopicRegistry
import java.util.concurrent.{Callable, ExecutorService, Executors, Future, TimeUnit}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

final case class ReplicatedAppendResult(errorCode: Short, baseOffset: Long)
private final case class ReplicaRecoverySession(followerId: Int, leaderEpoch: Int)

trait ReplicatedAppender:
  def append(
      topic: String,
      partition: Int,
      records: Array[Byte],
      acknowledgements: Short,
      timeoutMillis: Int
  ): ReplicatedAppendResult

/** Synchronous leader-to-follower record-batch replication with ISR high-watermark commits. */
final class ReplicationManager(
    config: BrokerConfig,
    cluster: ClusterManager,
    registry: TopicRegistry,
    peerClient: PeerClient
)
    extends ReplicatedAppender,
      AutoCloseable:
  private val executor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val partitionLocks = ConcurrentHashMap[String, Object]()
  private val recoveries = ConcurrentHashMap[String, ReplicaRecoverySession]()
  private val closed = AtomicBoolean(false)
  private val RecoveryChunkBytes = 8 * 1024 * 1024

  def append(
      topic: String,
      partition: Int,
      records: Array[Byte],
      acknowledgements: Short,
      timeoutMillis: Int
  ): ReplicatedAppendResult =
    if !cluster.isEnabled then
      registry.partition(topic, partition) match
        case Some(log) =>
          val result = log.append(records)
          ReplicatedAppendResult(Errors.None, result.baseOffset)
        case None => ReplicatedAppendResult(Errors.UnknownTopicOrPartition, -1L)
    else
      cluster.partition(topic, partition) match
        case None => ReplicatedAppendResult(Errors.UnknownTopicOrPartition, -1L)
        case Some(metadata) if metadata.leaderId != config.nodeId =>
          ReplicatedAppendResult(Errors.NotLeaderOrFollower, -1L)
        case Some(metadata) =>
          val lock = partitionLocks.computeIfAbsent(s"$topic-$partition", _ => Object())
          lock.synchronized {
            if recoveries.containsKey(partitionKey(topic, partition)) then
              ReplicatedAppendResult(Errors.ReplicaNotAvailable, -1L)
            else appendAsLeader(topic, partition, records, acknowledgements, timeoutMillis, metadata)
          }

  /**
   * Runs on the current leader. The partition lock fences Produce while the committed leader log
   * replaces the returning replica's local copy. A successful session stays fenced until the
   * controller commits ISR admission and explicitly releases it.
   */
  def recoverReplica(
      topic: String,
      partition: Int,
      followerId: Int,
      leaderEpoch: Int,
      timeoutMillis: Int
  ): Short =
    val key = partitionKey(topic, partition)
    val lock = partitionLocks.computeIfAbsent(key, _ => Object())
    lock.synchronized {
      cluster.partition(topic, partition) match
        case None => Errors.UnknownTopicOrPartition
        case Some(metadata) if metadata.leaderId != config.nodeId => Errors.NotLeaderOrFollower
        case Some(metadata) if metadata.leaderEpoch != leaderEpoch => Errors.FencedLeaderEpoch
        case Some(metadata) if !metadata.replicas.contains(followerId) || metadata.inSyncReplicas.contains(followerId) =>
          Errors.InvalidRequest
        case Some(_) =>
          registry.partition(topic, partition) match
            case None => Errors.UnknownTopicOrPartition
            case Some(log) =>
              nodeById(followerId) match
                case None => Errors.ReplicaNotAvailable
                case Some(follower) =>
                  try
                    copyCommittedLog(topic, partition, leaderEpoch, follower, log, timeoutMillis)
                    recoveries.put(key, ReplicaRecoverySession(followerId, leaderEpoch))
                    Errors.None
                  catch
                    case error: Throwable =>
                      System.err.println(
                        s"Cascade replica recovery failed for $topic-$partition on node $followerId: ${error.getMessage}"
                      )
                      Errors.ReplicaNotAvailable
    }

  /** Releases a recovery fence after ISR admission, or cancels it when metadata commit fails. */
  def completeReplicaRecovery(
      topic: String,
      partition: Int,
      followerId: Int,
      leaderEpoch: Int,
      admitted: Boolean
  ): Short =
    val key = partitionKey(topic, partition)
    val lock = partitionLocks.computeIfAbsent(key, _ => Object())
    lock.synchronized {
      Option(recoveries.get(key)) match
        case None => Errors.None
        case Some(session) if session != ReplicaRecoverySession(followerId, leaderEpoch) => Errors.InvalidRequest
        case Some(_) if admitted && !cluster.partition(topic, partition).exists { metadata =>
              metadata.leaderId == config.nodeId && metadata.leaderEpoch == leaderEpoch &&
              metadata.inSyncReplicas.contains(followerId)
            } => Errors.ReplicaNotAvailable
        case Some(_) =>
          recoveries.remove(key): Unit
          Errors.None
    }

  def handleInternal(apiKey: Short, cursor: ByteCursor): Array[Byte] = apiKey match
    case InternalApi.ReplicaAppend => replicaAppend(cursor)
    case InternalApi.ReplicaCommit => replicaCommit(cursor)
    case InternalApi.ReplicaCatchUp => replicaCatchUp(cursor)
    case InternalApi.ReplicaReset => replicaReset(cursor)
    case InternalApi.ReplicaRecoveryComplete => replicaRecoveryComplete(cursor)
    case _ => throw IllegalArgumentException(s"unsupported replication API: $apiKey")

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      executor.shutdownNow(): Unit
      executor.awaitTermination(5L, TimeUnit.SECONDS): Unit

  private def appendAsLeader(
      topic: String,
      partition: Int,
      records: Array[Byte],
      acknowledgements: Short,
      timeoutMillis: Int,
      metadata: PartitionMetadata
  ): ReplicatedAppendResult =
    if acknowledgements == -1 && metadata.inSyncReplicas.size < config.minInSyncReplicas then
      ReplicatedAppendResult(Errors.NotEnoughReplicas, -1L)
    else
      registry.partition(topic, partition) match
        case None => ReplicatedAppendResult(Errors.UnknownTopicOrPartition, -1L)
        case Some(log) =>
          val appended = log.appendReplica(records, log.logEndOffset)
          val followers = metadata.inSyncReplicas.filterNot(_ == config.nodeId).flatMap(nodeById)
          val appendPayload = ByteWriter(records.length + 128)
            .writeString(topic)
            .writeInt(partition)
            .writeInt(metadata.leaderEpoch)
            .writeLong(appended.baseOffset)
            .writeByteArray(records)
            .result()
          val acknowledgedFollowers = invokePeers(followers, timeoutMillis) { node =>
            val response = peerClient.call(node, InternalApi.ReplicaAppend, appendPayload, timeoutMillis)
            val accepted = response.readShort() == Errors.None
            response.readLong()
            response.ensureFullyRead()
            accepted
          }
          val allInSyncReplicasAcknowledged = acknowledgedFollowers.size + 1 >= metadata.inSyncReplicas.size
          if allInSyncReplicasAcknowledged then
            val committedOffset = Math.addExact(appended.lastOffset, 1L)
            log.commitThrough(committedOffset)
            val commitPayload = ByteWriter()
              .writeString(topic)
              .writeInt(partition)
              .writeInt(metadata.leaderEpoch)
              .writeLong(committedOffset)
              .result()
            invokePeers(acknowledgedFollowers, timeoutMillis) { node =>
              val response = peerClient.call(node, InternalApi.ReplicaCommit, commitPayload, timeoutMillis)
              val accepted = response.readShort() == Errors.None
              response.ensureFullyRead()
              accepted
            }: Unit

          if acknowledgements == -1 && !allInSyncReplicasAcknowledged then
            ReplicatedAppendResult(Errors.NotEnoughReplicasAfterAppend, appended.baseOffset)
          else ReplicatedAppendResult(Errors.None, appended.baseOffset)

  private def replicaAppend(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    val expectedBaseOffset = cursor.readLong()
    val records = cursor.readByteArray()
    cursor.ensureFullyRead()
    val (error, logEnd) = cluster.partition(topic, partition) match
      case None => (Errors.UnknownTopicOrPartition, -1L)
      case Some(metadata) if metadata.leaderEpoch != leaderEpoch => (Errors.FencedLeaderEpoch, -1L)
      case Some(metadata) if metadata.leaderId == config.nodeId || !metadata.replicas.contains(config.nodeId) =>
        (Errors.NotLeaderOrFollower, -1L)
      case Some(_) =>
        registry.partition(topic, partition) match
          case None => (Errors.UnknownTopicOrPartition, -1L)
          case Some(log) =>
            try
              log.appendReplica(records, expectedBaseOffset)
              (Errors.None, log.logEndOffset)
            catch case _: Throwable => (Errors.InvalidRequest, log.logEndOffset)
    ByteWriter().writeShort(error).writeLong(logEnd).result()

  private def replicaCommit(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    val committedOffset = cursor.readLong()
    cursor.ensureFullyRead()
    val error = cluster.partition(topic, partition) match
      case None => Errors.UnknownTopicOrPartition
      case Some(metadata) if metadata.leaderEpoch != leaderEpoch => Errors.FencedLeaderEpoch
      case Some(metadata) if !metadata.replicas.contains(config.nodeId) => Errors.NotLeaderOrFollower
      case Some(_) =>
        registry.partition(topic, partition) match
          case None => Errors.UnknownTopicOrPartition
          case Some(log) =>
            try
              log.commitThrough(committedOffset)
              Errors.None
            catch case _: Throwable => Errors.InvalidRequest
    ByteWriter().writeShort(error).result()

  private def replicaCatchUp(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val followerId = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    val timeoutMillis = cursor.readInt()
    cursor.ensureFullyRead()
    ByteWriter().writeShort(recoverReplica(topic, partition, followerId, leaderEpoch, timeoutMillis)).result()

  private def replicaReset(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val leaderId = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    val startOffset = cursor.readLong()
    cursor.ensureFullyRead()
    val error = cluster.partition(topic, partition) match
      case None => Errors.UnknownTopicOrPartition
      case Some(metadata) if metadata.leaderId != leaderId || metadata.leaderEpoch != leaderEpoch =>
        Errors.FencedLeaderEpoch
      case Some(metadata) if metadata.inSyncReplicas.contains(config.nodeId) || !metadata.replicas.contains(config.nodeId) =>
        Errors.InvalidRequest
      case Some(_) =>
        registry.partition(topic, partition) match
          case None => Errors.UnknownTopicOrPartition
          case Some(log) =>
            try
              log.resetReplica(startOffset)
              Errors.None
            catch case _: Throwable => Errors.ReplicaNotAvailable
    ByteWriter().writeShort(error).result()

  private def replicaRecoveryComplete(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val followerId = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    val admitted = cursor.readBoolean()
    cursor.ensureFullyRead()
    ByteWriter()
      .writeShort(completeReplicaRecovery(topic, partition, followerId, leaderEpoch, admitted))
      .result()

  private def copyCommittedLog(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      follower: ClusterNode,
      log: cascade.storage.PartitionLog,
      timeoutMillis: Int
  ): Unit =
    val startOffset = log.logStartOffset
    val targetHighWatermark = log.highWatermark
    val resetPayload = ByteWriter()
      .writeString(topic)
      .writeInt(partition)
      .writeInt(config.nodeId)
      .writeInt(leaderEpoch)
      .writeLong(startOffset)
      .result()
    val reset =
      try peerClient.call(follower, InternalApi.ReplicaReset, resetPayload, timeoutMillis)
      catch case _: Throwable =>
        // Reset is idempotent, so retry once after PeerClient discards a stale pre-restart socket.
        peerClient.call(follower, InternalApi.ReplicaReset, resetPayload, timeoutMillis)
    val resetError = reset.readShort()
    reset.ensureFullyRead()
    if resetError != Errors.None then throw IllegalStateException(s"replica reset returned error $resetError")

    var nextOffset = startOffset
    while nextOffset < targetHighWatermark do
      val records = log.fetch(nextOffset, RecoveryChunkBytes, targetHighWatermark, _ => true).records
      if records.isEmpty then
        throw IllegalStateException(s"leader returned no records before high watermark $targetHighWatermark")
      val append = peerClient.call(
        follower,
        InternalApi.ReplicaAppend,
        ByteWriter(records.length + 128)
          .writeString(topic)
          .writeInt(partition)
          .writeInt(leaderEpoch)
          .writeLong(nextOffset)
          .writeByteArray(records)
          .result(),
        timeoutMillis
      )
      val appendError = append.readShort()
      val followerLogEnd = append.readLong()
      append.ensureFullyRead()
      if appendError != Errors.None || followerLogEnd <= nextOffset || followerLogEnd > targetHighWatermark then
        throw IllegalStateException(
          s"replica append failed: error=$appendError, previous=$nextOffset, returned=$followerLogEnd"
        )
      nextOffset = followerLogEnd

    val commit = peerClient.call(
      follower,
      InternalApi.ReplicaCommit,
      ByteWriter()
        .writeString(topic)
        .writeInt(partition)
        .writeInt(leaderEpoch)
        .writeLong(targetHighWatermark)
        .result(),
      timeoutMillis
    )
    val commitError = commit.readShort()
    commit.ensureFullyRead()
    if commitError != Errors.None then throw IllegalStateException(s"replica commit returned error $commitError")

  private def nodeById(id: Int): Option[ClusterNode] = cluster.clusterNodes.find(_.id == id)

  private def partitionKey(topic: String, partition: Int): String = s"$topic-$partition"

  private def invokePeers(
      nodes: Vector[ClusterNode],
      timeoutMillis: Int
  )(operation: ClusterNode => Boolean): Vector[ClusterNode] =
    val futures: Vector[(ClusterNode, Future[Boolean])] = nodes.map { node =>
      node -> executor.submit(new Callable[Boolean]:
        override def call(): Boolean =
          try operation(node)
          catch case _: Throwable => false
      )
    }
    val deadline = System.nanoTime() + timeoutMillis.toLong * 1_000_000L
    futures.flatMap { case (node, future) =>
      val remaining = deadline - System.nanoTime()
      if remaining <= 0L then
        future.cancel(true): Unit
        None
      else
        try Option.when(future.get(remaining, TimeUnit.NANOSECONDS))(node)
        catch
          case _: Throwable =>
            future.cancel(true): Unit
            None
    }

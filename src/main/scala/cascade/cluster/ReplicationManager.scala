package cascade.cluster

import cascade.broker.BrokerConfig
import cascade.protocol.{ByteCursor, ByteWriter, Errors}
import cascade.storage.{BatchFingerprint, TopicRegistry}
import java.util.concurrent.{Callable, ExecutorService, Executors, Future, TimeUnit}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

final case class ReplicatedAppendResult(errorCode: Short, baseOffset: Long)
private final case class ReplicaRecoveryState(errorCode: Short, logStart: Long, logEnd: Long, highWatermark: Long)
private final case class ReplicaRecoveryProbe(
    errorCode: Short,
    fingerprint: Option[BatchFingerprint]
)

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
    peerClient: PeerTransport
)
    extends ReplicatedAppender,
      AutoCloseable:
  private val executor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val partitionLocks = ConcurrentHashMap[String, Object]()
  private val recoveries = ConcurrentHashMap[String, ConcurrentHashMap[Int, Int]]()
  private val closed = AtomicBoolean(false)

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
    else if cluster.isBrokerFenced then ReplicatedAppendResult(Errors.BrokerNotAvailable, -1L)
    else
      cluster.partition(topic, partition) match
        case None => ReplicatedAppendResult(Errors.UnknownTopicOrPartition, -1L)
        case Some(metadata) if metadata.leaderId != config.nodeId =>
          ReplicatedAppendResult(Errors.NotLeaderOrFollower, -1L)
        case Some(metadata) =>
          val lock = partitionLocks.computeIfAbsent(s"$topic-$partition", _ => Object())
          lock.synchronized {
            if Option(recoveries.get(partitionKey(topic, partition))).exists(!_.isEmpty) then
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
    recoveryAccess(topic, partition, followerId, leaderEpoch) match
      case denied if denied != Errors.None => denied
      case _ =>
        (registry.partition(topic, partition), nodeById(followerId)) match
          case (None, _) => Errors.UnknownTopicOrPartition
          case (_, None) => Errors.ReplicaNotAvailable
          case (Some(log), Some(follower)) =>
            try
              // Copy the bulk of the committed log while Produce continues through the existing ISR.
              copyCommittedLog(topic, partition, leaderEpoch, follower, log, timeoutMillis)
              lock.synchronized {
                recoveryAccess(topic, partition, followerId, leaderEpoch) match
                  case denied if denied != Errors.None => denied
                  case _ =>
                    // Fence only the final delta and ISR metadata transition.
                    copyCommittedLog(topic, partition, leaderEpoch, follower, log, timeoutMillis)
                    recoveries.computeIfAbsent(key, _ => ConcurrentHashMap[Int, Int]()).put(followerId, leaderEpoch): Unit
                    Errors.None
              }
            catch
              case error: Throwable =>
                System.err.println(
                  s"Cascade replica recovery failed for $topic-$partition on node $followerId: ${error.getMessage}"
                )
                Errors.ReplicaNotAvailable

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
      Option(recoveries.get(key)).flatMap(sessions => Option(sessions.get(followerId))) match
        case None => Errors.None
        case Some(epoch) if epoch != leaderEpoch => Errors.InvalidRequest
        case Some(_) if admitted && !cluster.partition(topic, partition).exists { metadata =>
              metadata.leaderId == config.nodeId && metadata.leaderEpoch == leaderEpoch &&
              metadata.inSyncReplicas.contains(followerId)
            } => Errors.ReplicaNotAvailable
        case Some(_) =>
          val sessions = recoveries.get(key)
          if sessions != null then
            sessions.remove(followerId): Unit
            if sessions.isEmpty then recoveries.remove(key, sessions): Unit
          Errors.None
    }

  private def recoveryAccess(topic: String, partition: Int, followerId: Int, leaderEpoch: Int): Short =
    if cluster.isBrokerFenced then Errors.BrokerNotAvailable
    else cluster.partition(topic, partition) match
      case None => Errors.UnknownTopicOrPartition
      case Some(metadata) if metadata.leaderId != config.nodeId => Errors.NotLeaderOrFollower
      case Some(metadata) if metadata.leaderEpoch != leaderEpoch => Errors.FencedLeaderEpoch
      case Some(metadata) if !metadata.replicas.contains(followerId) || metadata.inSyncReplicas.contains(followerId) =>
        Errors.InvalidRequest
      case Some(_) => Errors.None

  def handleInternal(apiKey: Short, cursor: ByteCursor): Array[Byte] = apiKey match
    case InternalApi.ReplicaAppend => replicaAppend(cursor)
    case InternalApi.ReplicaCommit => replicaCommit(cursor)
    case InternalApi.ReplicaCatchUp => replicaCatchUp(cursor)
    case InternalApi.ReplicaReset => replicaReset(cursor)
    case InternalApi.ReplicaRecoveryComplete => replicaRecoveryComplete(cursor)
    case InternalApi.ReplicaRecoveryState => replicaRecoveryState(cursor)
    case InternalApi.ReplicaRecoveryProbe => replicaRecoveryProbe(cursor)
    case InternalApi.ReplicaTruncate => replicaTruncate(cursor)
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
            rollbackUncommittedAppend(
              topic,
              partition,
              metadata.leaderEpoch,
              appended.baseOffset,
              followers,
              log,
              math.min(timeoutMillis, config.peerTimeoutMillis)
            )

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
    val (error, logEnd) = if cluster.isBrokerFenced then (Errors.BrokerNotAvailable, -1L)
    else cluster.partition(topic, partition) match
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
    val error = if cluster.isBrokerFenced then Errors.BrokerNotAvailable
    else cluster.partition(topic, partition) match
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
    val error = if cluster.isBrokerFenced then Errors.BrokerNotAvailable
    else cluster.partition(topic, partition) match
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

  private def replicaRecoveryState(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val leaderId = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    cursor.ensureFullyRead()
    val result = replicaRecoveryAccess(topic, partition, leaderId, leaderEpoch) match
      case error if error != Errors.None => ReplicaRecoveryState(error, -1L, -1L, -1L)
      case _ =>
        registry.partition(topic, partition) match
          case None => ReplicaRecoveryState(Errors.UnknownTopicOrPartition, -1L, -1L, -1L)
          case Some(log) => ReplicaRecoveryState(Errors.None, log.logStartOffset, log.logEndOffset, log.highWatermark)
    ByteWriter()
      .writeShort(result.errorCode)
      .writeLong(result.logStart)
      .writeLong(result.logEnd)
      .writeLong(result.highWatermark)
      .result()

  private def replicaRecoveryProbe(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val leaderId = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    val offsetInclusive = cursor.readLong()
    val endOffsetExclusive = cursor.readLong()
    cursor.ensureFullyRead()
    val result = replicaRecoveryAccess(topic, partition, leaderId, leaderEpoch) match
      case error if error != Errors.None => ReplicaRecoveryProbe(error, None)
      case _ =>
        registry.partition(topic, partition) match
          case None => ReplicaRecoveryProbe(Errors.UnknownTopicOrPartition, None)
          case Some(log) =>
            try ReplicaRecoveryProbe(Errors.None, log.recoveryProbe(offsetInclusive, endOffsetExclusive))
            catch case _: Throwable => ReplicaRecoveryProbe(Errors.InvalidRequest, None)
    val writer = ByteWriter().writeShort(result.errorCode).writeBoolean(result.fingerprint.nonEmpty)
    result.fingerprint.foreach(fingerprint => writeFingerprint(writer, fingerprint))
    writer.result()

  private def replicaTruncate(cursor: ByteCursor): Array[Byte] =
    val topic = cursor.readString()
    val partition = cursor.readInt()
    val leaderId = cursor.readInt()
    val leaderEpoch = cursor.readInt()
    val offsetExclusive = cursor.readLong()
    cursor.ensureFullyRead()
    val error = replicaTruncationAccess(topic, partition, leaderId, leaderEpoch) match
      case denied if denied != Errors.None => denied
      case _ =>
        registry.partition(topic, partition) match
          case None => Errors.UnknownTopicOrPartition
          case Some(log) =>
            try
              log.truncateReplicaTo(offsetExclusive)
              Errors.None
            catch case _: Throwable => Errors.InvalidRequest
    ByteWriter().writeShort(error).result()

  private def replicaTruncationAccess(topic: String, partition: Int, leaderId: Int, leaderEpoch: Int): Short =
    if cluster.isBrokerFenced then Errors.BrokerNotAvailable
    else cluster.partition(topic, partition) match
      case None => Errors.UnknownTopicOrPartition
      case Some(metadata) if metadata.leaderId != leaderId || metadata.leaderEpoch != leaderEpoch =>
        Errors.FencedLeaderEpoch
      case Some(metadata) if metadata.leaderId == config.nodeId || !metadata.replicas.contains(config.nodeId) =>
        Errors.InvalidRequest
      case Some(_) => Errors.None

  private def replicaRecoveryAccess(topic: String, partition: Int, leaderId: Int, leaderEpoch: Int): Short =
    if cluster.isBrokerFenced then Errors.BrokerNotAvailable
    else cluster.partition(topic, partition) match
      case None => Errors.UnknownTopicOrPartition
      case Some(metadata) if metadata.leaderId != leaderId || metadata.leaderEpoch != leaderEpoch =>
        Errors.FencedLeaderEpoch
      case Some(metadata) if metadata.inSyncReplicas.contains(config.nodeId) || !metadata.replicas.contains(config.nodeId) =>
        Errors.InvalidRequest
      case Some(_) => Errors.None

  private def writeFingerprint(writer: ByteWriter, fingerprint: BatchFingerprint): Unit =
    writer
      .writeLong(fingerprint.baseOffset)
      .writeLong(fingerprint.lastOffset)
      .writeInt(fingerprint.size)
      .writeLong(fingerprint.digestHigh)
      .writeLong(fingerprint.digestLow): Unit

  private def rollbackUncommittedAppend(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      baseOffset: Long,
      followers: Vector[ClusterNode],
      log: cascade.storage.PartitionLog,
      timeoutMillis: Int
  ): Unit =
    val payload = ByteWriter()
      .writeString(topic)
      .writeInt(partition)
      .writeInt(config.nodeId)
      .writeInt(leaderEpoch)
      .writeLong(baseOffset)
      .result()
    invokePeers(followers, timeoutMillis) { follower =>
      val response = peerClient.call(follower, InternalApi.ReplicaTruncate, payload, timeoutMillis)
      val accepted = response.readShort() == Errors.None
      response.ensureFullyRead()
      accepted
    }: Unit
    log.truncateReplicaTo(baseOffset)

  private def copyCommittedLog(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      follower: ClusterNode,
      log: cascade.storage.PartitionLog,
      timeoutMillis: Int
  ): Unit =
    val leaderStart = log.logStartOffset
    val targetHighWatermark = log.highWatermark
    val followerState = readRecoveryState(topic, partition, leaderEpoch, follower, timeoutMillis)
    if followerState.errorCode != Errors.None then
      throw IllegalStateException(s"replica recovery state returned error ${followerState.errorCode}")
    val commonOffset = findCommonOffset(
      topic,
      partition,
      leaderEpoch,
      follower,
      log,
      leaderStart,
      targetHighWatermark,
      followerState,
      timeoutMillis
    )
    if commonOffset < followerState.logStart then
      resetFollower(topic, partition, leaderEpoch, follower, leaderStart, timeoutMillis)
    else truncateFollower(topic, partition, leaderEpoch, follower, commonOffset, timeoutMillis)

    var nextOffset = commonOffset
    while nextOffset < targetHighWatermark do
      val records = log.fetch(nextOffset, config.replicaRecoveryChunkBytes, targetHighWatermark, _ => true).records
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

  private def readRecoveryState(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      follower: ClusterNode,
      timeoutMillis: Int
  ): ReplicaRecoveryState =
    val payload = ByteWriter()
      .writeString(topic)
      .writeInt(partition)
      .writeInt(config.nodeId)
      .writeInt(leaderEpoch)
      .result()
    val response = callRecoveryPeer(follower, InternalApi.ReplicaRecoveryState, payload, timeoutMillis)
    val state = ReplicaRecoveryState(response.readShort(), response.readLong(), response.readLong(), response.readLong())
    response.ensureFullyRead()
    state

  private def findCommonOffset(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      follower: ClusterNode,
      log: cascade.storage.PartitionLog,
      leaderStart: Long,
      targetHighWatermark: Long,
      followerState: ReplicaRecoveryState,
      timeoutMillis: Int
  ): Long =
    val overlapStart = math.max(leaderStart, followerState.logStart)
    val overlapEnd = math.min(targetHighWatermark, followerState.logEnd)
    if overlapEnd <= overlapStart then leaderStart
    else
      var low = overlapStart
      var high = overlapEnd
      var common = leaderStart
      while low < high do
        val midpoint = low + (high - low - 1L) / 2L
        val leaderProbe = log.recoveryProbe(midpoint, overlapEnd)
        leaderProbe match
          case None => high = midpoint
          case Some(expected) =>
            val actual = probeFollower(
              topic,
              partition,
              leaderEpoch,
              follower,
              expected.baseOffset,
              overlapEnd,
              timeoutMillis
            )
            if actual.errorCode != Errors.None then
              throw IllegalStateException(s"replica recovery probe returned error ${actual.errorCode}")
            if actual.fingerprint.contains(expected) then
              common = Math.addExact(expected.lastOffset, 1L)
              low = common
            else high = expected.baseOffset
      common

  private def probeFollower(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      follower: ClusterNode,
      offsetInclusive: Long,
      endOffsetExclusive: Long,
      timeoutMillis: Int
  ): ReplicaRecoveryProbe =
    val payload = ByteWriter()
      .writeString(topic)
      .writeInt(partition)
      .writeInt(config.nodeId)
      .writeInt(leaderEpoch)
      .writeLong(offsetInclusive)
      .writeLong(endOffsetExclusive)
      .result()
    val response = callRecoveryPeer(follower, InternalApi.ReplicaRecoveryProbe, payload, timeoutMillis)
    val error = response.readShort()
    val fingerprint = Option.when(response.readBoolean()) {
      BatchFingerprint(
        response.readLong(),
        response.readLong(),
        response.readInt(),
        response.readLong(),
        response.readLong()
      )
    }
    response.ensureFullyRead()
    ReplicaRecoveryProbe(error, fingerprint)

  private def truncateFollower(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      follower: ClusterNode,
      offsetExclusive: Long,
      timeoutMillis: Int
  ): Unit =
    val payload = ByteWriter()
      .writeString(topic)
      .writeInt(partition)
      .writeInt(config.nodeId)
      .writeInt(leaderEpoch)
      .writeLong(offsetExclusive)
      .result()
    val response = callRecoveryPeer(follower, InternalApi.ReplicaTruncate, payload, timeoutMillis)
    val error = response.readShort()
    response.ensureFullyRead()
    if error != Errors.None then throw IllegalStateException(s"replica truncate returned error $error")

  private def resetFollower(
      topic: String,
      partition: Int,
      leaderEpoch: Int,
      follower: ClusterNode,
      startOffset: Long,
      timeoutMillis: Int
  ): Unit =
    val payload = ByteWriter()
      .writeString(topic)
      .writeInt(partition)
      .writeInt(config.nodeId)
      .writeInt(leaderEpoch)
      .writeLong(startOffset)
      .result()
    val response = callRecoveryPeer(follower, InternalApi.ReplicaReset, payload, timeoutMillis)
    val error = response.readShort()
    response.ensureFullyRead()
    if error != Errors.None then throw IllegalStateException(s"replica reset returned error $error")

  private def callRecoveryPeer(
      follower: ClusterNode,
      apiKey: Short,
      payload: Array[Byte],
      timeoutMillis: Int
  ): ByteCursor =
    try peerClient.call(follower, apiKey, payload, timeoutMillis)
    catch case _: Throwable =>
      // Recovery operations are idempotent and the transport may still own a pre-restart socket.
      peerClient.call(follower, apiKey, payload, timeoutMillis)

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

package cascade.cluster

import cascade.broker.BrokerConfig
import cascade.protocol.{ByteCursor, ByteWriter, Errors}
import cascade.storage.TopicRegistry
import java.util.concurrent.{Callable, ExecutorService, Executors, Future, TimeUnit}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

final case class ReplicatedAppendResult(errorCode: Short, baseOffset: Long)

/** Synchronous leader-to-follower record-batch replication with ISR high-watermark commits. */
final class ReplicationManager(
    config: BrokerConfig,
    cluster: ClusterManager,
    registry: TopicRegistry,
    peerClient: PeerClient
)
    extends AutoCloseable:
  private val executor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val partitionLocks = ConcurrentHashMap[String, Object]()
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
    else
      cluster.partition(topic, partition) match
        case None => ReplicatedAppendResult(Errors.UnknownTopicOrPartition, -1L)
        case Some(metadata) if metadata.leaderId != config.nodeId =>
          ReplicatedAppendResult(Errors.NotLeaderOrFollower, -1L)
        case Some(metadata) =>
          val lock = partitionLocks.computeIfAbsent(s"$topic-$partition", _ => Object())
          lock.synchronized {
            appendAsLeader(topic, partition, records, acknowledgements, timeoutMillis, metadata)
          }

  def handleInternal(apiKey: Short, cursor: ByteCursor): Array[Byte] = apiKey match
    case InternalApi.ReplicaAppend => replicaAppend(cursor)
    case InternalApi.ReplicaCommit => replicaCommit(cursor)
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

  private def nodeById(id: Int): Option[ClusterNode] = cluster.clusterNodes.find(_.id == id)

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

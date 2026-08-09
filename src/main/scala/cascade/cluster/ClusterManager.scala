package cascade.cluster

import cascade.broker.BrokerConfig
import cascade.protocol.{ByteCursor, ByteWriter, Errors}
import cascade.storage.{CreateTopicResult, TopicRegistry}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

final case class ClusterCreateResult(errorCode: Short, message: Option[String])
private final case class ReplicaRecoveryTarget(
    topic: String,
    partition: Int,
    leaderId: Int,
    leaderEpoch: Int,
    followerId: Int
)

/**
 * Static-membership metadata quorum. A fixed controller commits metadata images to a majority;
 * controller election and replica re-admission deliberately remain outside this milestone.
 */
final class ClusterManager(config: BrokerConfig, registry: TopicRegistry, localNode: ClusterNode, peerClient: PeerClient)
    extends AutoCloseable:
  private val enabled = config.clusterNodes.nonEmpty
  private val nodes = if enabled then config.clusterNodes.sortBy(_.id) else Vector(localNode)
  private val nodeById = nodes.map(node => node.id -> node).toMap
  private val quorumSize = nodes.size / 2 + 1
  private val closed = AtomicBoolean(false)
  private val store =
    Option.when(enabled)(MetadataStore(config.dataDirectory.resolve(".cascade").resolve("cluster-metadata.log")))
  @volatile private var current = store.map(_.metadata).getOrElse(ClusterMetadata.Empty)
  @volatile private var controllerReady = !enabled || config.nodeId != config.controllerId
  @volatile private var replicationManager: ReplicationManager | Null = null
  private val missedHeartbeats = mutable.HashMap.empty[Int, Int]
  private val pendingRecoveryReleases = mutable.HashMap.empty[ReplicaRecoveryTarget, Boolean]
  private val monitor: Option[ScheduledExecutorService] = Option.when(enabled && config.nodeId == config.controllerId) {
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-cluster-monitor").factory())
  }

  applyMetadata(current)

  def attachReplicationManager(manager: ReplicationManager): Unit = synchronized {
    if replicationManager != null then throw IllegalStateException("replication manager is already attached")
    replicationManager = manager
  }

  def start(): Unit =
    if enabled && replicationManager == null then throw IllegalStateException("replication manager is not attached")
    if enabled && config.nodeId != config.controllerId then synchronizeFromController()
    else if enabled then recoverControllerState()
    monitor.foreach { executor =>
      executor.scheduleWithFixedDelay(() => monitorPeers(), 500L, 500L, TimeUnit.MILLISECONDS): Unit
    }

  def isEnabled: Boolean = enabled

  def clusterNodes: Vector[ClusterNode] = nodes

  def controllerNode: ClusterNode = nodeById(config.controllerId)

  def topicNames: Vector[String] = if enabled then current.topics.map(_.name).sorted else registry.topicNames

  def topic(name: String): Option[TopicMetadata] = if enabled then current.byName.get(name) else None

  def partition(topic: String, partition: Int): Option[PartitionMetadata] =
    this.topic(topic).flatMap(_.partitions.lift(partition))

  def validateTopic(name: String, partitions: Int, replicationFactor: Int): ClusterCreateResult = synchronized {
    if !registry.validateTopicName(name) then ClusterCreateResult(Errors.InvalidTopic, Some("invalid topic name"))
    else if partitions <= 0 then ClusterCreateResult(Errors.InvalidPartitions, Some("partition count must be positive"))
    else if replicationFactor <= 0 || replicationFactor > nodes.size then
      ClusterCreateResult(Errors.InvalidReplicationFactor, Some("replication factor exceeds the configured cluster"))
    else if (if enabled then current.byName.contains(name) else registry.partitions(name).nonEmpty) then
      ClusterCreateResult(Errors.TopicAlreadyExists, Some(s"Topic '$name' already exists"))
    else ClusterCreateResult(Errors.None, None)
  }

  def createTopic(name: String, partitions: Int, replicationFactor: Int): ClusterCreateResult =
    if !enabled then
      if replicationFactor != 1 then
        ClusterCreateResult(Errors.InvalidReplicationFactor, Some("single-node mode requires replication factor 1"))
      else localCreate(name, partitions)
    else if config.nodeId == config.controllerId then synchronized {
      createOnController(name, partitions, replicationFactor)
    }
    else
      try
        val response = peerClient.call(
          controllerNode,
          InternalApi.CreateTopic,
          ByteWriter().writeString(name).writeInt(partitions).writeInt(replicationFactor).result(),
          config.peerTimeoutMillis
        )
        val result = ClusterCreateResult(response.readShort(), response.readNullableString())
        response.ensureFullyRead()
        if result.errorCode == Errors.None then synchronizeFromController()
        result
      catch
        case error: Throwable => ClusterCreateResult(Errors.CoordinatorNotAvailable, Some(error.getMessage))

  def handleInternal(apiKey: Short, cursor: ByteCursor): Array[Byte] = apiKey match
    case InternalApi.Ping =>
      cursor.ensureFullyRead()
      ByteWriter().writeShort(Errors.None).result()
    case InternalApi.MetadataPrepare =>
      val version = cursor.readLong()
      cursor.ensureFullyRead()
      val accepted = synchronized(version > current.version)
      ByteWriter().writeShort(if accepted then Errors.None else Errors.InvalidRequest).result()
    case InternalApi.MetadataCommit =>
      val metadata = MetadataCodec.decode(cursor.readByteArray())
      cursor.ensureFullyRead()
      val accepted = synchronized {
        if metadata.version > current.version then
          commitLocal(metadata)
          true
        else metadata.version == current.version
      }
      ByteWriter().writeShort(if accepted then Errors.None else Errors.InvalidRequest).result()
    case InternalApi.MetadataSnapshot =>
      cursor.ensureFullyRead()
      ByteWriter().writeByteArray(MetadataCodec.encode(current)).result()
    case InternalApi.CreateTopic =>
      val name = cursor.readString()
      val partitions = cursor.readInt()
      val replicationFactor = cursor.readInt()
      cursor.ensureFullyRead()
      val result =
        if config.nodeId == config.controllerId then synchronized(createOnController(name, partitions, replicationFactor))
        else ClusterCreateResult(Errors.NotController, Some("metadata mutation must be sent to the controller"))
      ByteWriter().writeShort(result.errorCode).writeNullableString(result.message).result()
    case _ => throw IllegalArgumentException(s"unsupported metadata API: $apiKey")

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      monitor.foreach { executor =>
        executor.shutdownNow(): Unit
        executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
      }
      store.foreach(_.close())

  private def createOnController(name: String, partitions: Int, replicationFactor: Int): ClusterCreateResult =
    if !controllerReady then recoverControllerState()
    if !controllerReady then
      ClusterCreateResult(Errors.CoordinatorLoadInProgress, Some("controller is recovering metadata from the quorum"))
    else validateTopic(name, partitions, replicationFactor) match
      case error if error.errorCode != Errors.None => error
      case _ =>
        val assignments = Vector.tabulate(partitions) { partition =>
          val replicas = Vector.tabulate(replicationFactor)(offset => nodes((partition + offset) % nodes.size).id)
          PartitionMetadata(partition, replicas.head, 0, replicas, replicas)
        }
        val next = ClusterMetadata(
          Math.addExact(current.version, 1L),
          (current.topics :+ TopicMetadata(name, assignments)).sortBy(_.name)
        )
        if propose(next) then ClusterCreateResult(Errors.None, None)
        else ClusterCreateResult(Errors.CoordinatorNotAvailable, Some("metadata quorum is unavailable"))

  private def localCreate(name: String, partitions: Int): ClusterCreateResult =
    registry.createTopic(name, partitions) match
      case CreateTopicResult.Created => ClusterCreateResult(Errors.None, None)
      case CreateTopicResult.AlreadyExists =>
        ClusterCreateResult(Errors.TopicAlreadyExists, Some(s"Topic '$name' already exists"))
      case CreateTopicResult.InvalidPartitions =>
        ClusterCreateResult(Errors.InvalidPartitions, Some("partition count must be positive"))
      case CreateTopicResult.InvalidName => ClusterCreateResult(Errors.InvalidTopic, Some("invalid topic name"))

  private def propose(next: ClusterMetadata): Boolean =
    val preparedPeers = nodes.iterator.filterNot(_.id == config.nodeId).flatMap { node =>
      try
        val response = peerClient.call(
          node,
          InternalApi.MetadataPrepare,
          ByteWriter().writeLong(next.version).result(),
          config.peerTimeoutMillis
        )
        val accepted = response.readShort() == Errors.None
        response.ensureFullyRead()
        Option.when(accepted)(node)
      catch case _: Throwable => None
    }.toVector
    if preparedPeers.size + 1 < quorumSize then false
    else
      val encoded = MetadataCodec.encode(next)
      val committedPeers = preparedPeers.count { node =>
        try
          val response = peerClient.call(
            node,
            InternalApi.MetadataCommit,
            ByteWriter().writeByteArray(encoded).result(),
            config.peerTimeoutMillis
          )
          val committed = response.readShort() == Errors.None
          response.ensureFullyRead()
          committed
        catch case _: Throwable => false
      }
      if committedPeers + 1 >= quorumSize then
        commitLocal(next)
        true
      else false

  private def commitLocal(metadata: ClusterMetadata): Unit = synchronized {
    store.foreach(_.commit(metadata))
    current = metadata
    applyMetadata(metadata)
  }

  private def applyMetadata(metadata: ClusterMetadata): Unit =
    metadata.topics.foreach { topic =>
      registry.partitions(topic.name) match
        case Some(existing) if existing.size != topic.partitions.size =>
          throw IllegalStateException(s"local topic ${topic.name} has ${existing.size} partitions; metadata requires ${topic.partitions.size}")
        case Some(_) => ()
        case None =>
          registry.createTopic(topic.name, topic.partitions.size) match
            case CreateTopicResult.Created | CreateTopicResult.AlreadyExists => ()
            case other => throw IllegalStateException(s"cannot materialize metadata for ${topic.name}: $other")
    }

  private def synchronizeFromController(): Unit =
    try
      val response = peerClient.call(controllerNode, InternalApi.MetadataSnapshot, Array.emptyByteArray, config.peerTimeoutMillis)
      val metadata = MetadataCodec.decode(response.readByteArray())
      response.ensureFullyRead()
      if metadata.version > current.version then commitLocal(metadata)
    catch
      case error: Throwable => System.err.println(s"Cascade metadata sync failed: ${error.getMessage}")

  private def monitorPeers(): Unit =
    if !closed.get() then
      if !controllerReady then recoverControllerState()
      retryPendingRecoveryReleases()
      nodes.filterNot(_.id == config.nodeId).foreach { node =>
        val healthy =
          try
            val response = peerClient.call(node, InternalApi.Ping, Array.emptyByteArray, math.min(1000, config.peerTimeoutMillis))
            val result = response.readShort() == Errors.None
            response.ensureFullyRead()
            result
          catch case _: Throwable => false
        val misses = synchronized {
          val value = if healthy then 0 else missedHeartbeats.getOrElse(node.id, 0) + 1
          missedHeartbeats.update(node.id, value)
          value
        }
        if healthy && nodeNeedsRecovery(node.id) && synchronizeNode(node) then recoverNode(node.id)
        else if misses >= 3 then removeFailedNode(node.id)
      }

  private def recoverControllerState(): Unit =
    val snapshots = current +: nodes.filterNot(_.id == config.nodeId).flatMap { node =>
      try
        val response = peerClient.call(node, InternalApi.MetadataSnapshot, Array.emptyByteArray, config.peerTimeoutMillis)
        val metadata = MetadataCodec.decode(response.readByteArray())
        response.ensureFullyRead()
        Some(metadata)
      catch case _: Throwable => None
    }
    val committed = snapshots.groupBy(identity).valuesIterator
      .filter(_.size >= quorumSize)
      .map(_.head)
      .maxByOption(_.version)
    committed.foreach { metadata =>
      if metadata.version > current.version then commitLocal(metadata)
      controllerReady = true
    }

  private def removeFailedNode(nodeId: Int): Unit = synchronized {
    val changedTopics = current.topics.map { topic =>
      val changedPartitions = topic.partitions.map { partition =>
        if partition.inSyncReplicas.contains(nodeId) then
          val remaining = partition.inSyncReplicas.filterNot(_ == nodeId)
          val leader = if partition.leaderId == nodeId then remaining.headOption.getOrElse(-1) else partition.leaderId
          partition.copy(
            leaderId = leader,
            leaderEpoch = Math.addExact(partition.leaderEpoch, 1),
            inSyncReplicas = remaining
          )
        else partition
      }
      topic.copy(partitions = changedPartitions)
    }
    if changedTopics != current.topics then
      val next = ClusterMetadata(Math.addExact(current.version, 1L), changedTopics)
      if !propose(next) then System.err.println(s"Cascade could not commit metadata failover for node $nodeId")
  }

  private def synchronizeNode(node: ClusterNode): Boolean =
    try
      val response = peerClient.call(
        node,
        InternalApi.MetadataCommit,
        ByteWriter().writeByteArray(MetadataCodec.encode(current)).result(),
        config.peerTimeoutMillis
      )
      val accepted = response.readShort() == Errors.None
      response.ensureFullyRead()
      accepted
    catch case _: Throwable => false

  private def recoverNode(nodeId: Int): Unit =
    val targets = synchronized {
      current.topics.flatMap { topic =>
        topic.partitions.collect {
          case partition
              if partition.replicas.contains(nodeId) && !partition.inSyncReplicas.contains(nodeId) &&
                partition.leaderId >= 0 =>
            ReplicaRecoveryTarget(topic.name, partition.partition, partition.leaderId, partition.leaderEpoch, nodeId)
        }
      }
    }
    val results = targets.map(target => target -> requestReplicaRecovery(target))
    results.collect { case (target, error) if error != Errors.None => target }.foreach { target =>
      if !releaseReplicaRecovery(target, admitted = false) then
        synchronized(pendingRecoveryReleases.update(target, false))
    }
    val recovered = results.collect { case (target, Errors.None) => target }
    if recovered.nonEmpty then
      val recoveredKeys = recovered.map(target => (target.topic, target.partition) -> target).toMap
      val committed = synchronized {
        val changedTopics = current.topics.map { topic =>
          val changedPartitions = topic.partitions.map { partition =>
            recoveredKeys.get((topic.name, partition.partition)) match
              case Some(target)
                  if partition.leaderId == target.leaderId && partition.leaderEpoch == target.leaderEpoch &&
                    partition.replicas.contains(nodeId) && !partition.inSyncReplicas.contains(nodeId) =>
                val admitted = partition.replicas.filter(id => partition.inSyncReplicas.contains(id) || id == nodeId)
                partition.copy(inSyncReplicas = admitted)
              case _ => partition
          }
          topic.copy(partitions = changedPartitions)
        }
        if changedTopics == current.topics then false
        else propose(ClusterMetadata(Math.addExact(current.version, 1L), changedTopics))
      }
      recovered.foreach { target =>
        if releaseReplicaRecovery(target, admitted = committed) then
          synchronized(pendingRecoveryReleases.remove(target): Unit)
        else synchronized(pendingRecoveryReleases.update(target, committed))
      }

  private def nodeNeedsRecovery(nodeId: Int): Boolean = synchronized {
    current.topics.exists(_.partitions.exists { partition =>
      partition.leaderId >= 0 && partition.replicas.contains(nodeId) && !partition.inSyncReplicas.contains(nodeId)
    })
  }

  private def retryPendingRecoveryReleases(): Unit =
    val pending = synchronized(pendingRecoveryReleases.toVector)
    pending.foreach { case (target, requestedAdmission) =>
      val stillAdmitted = synchronized {
        current.byName.get(target.topic).flatMap(_.partitions.lift(target.partition)).exists { partition =>
          partition.leaderId == target.leaderId && partition.leaderEpoch == target.leaderEpoch &&
          partition.inSyncReplicas.contains(target.followerId)
        }
      }
      if releaseReplicaRecovery(target, admitted = requestedAdmission && stillAdmitted) then
        synchronized(pendingRecoveryReleases.remove(target): Unit)
    }

  private def requestReplicaRecovery(target: ReplicaRecoveryTarget): Short =
    if target.leaderId == config.nodeId then
      Option(replicationManager) match
        case Some(manager) =>
          manager.recoverReplica(
            target.topic,
            target.partition,
            target.followerId,
            target.leaderEpoch,
            config.peerTimeoutMillis
          )
        case None => Errors.ReplicaNotAvailable
    else
      nodeById.get(target.leaderId) match
        case None => Errors.ReplicaNotAvailable
        case Some(leader) =>
          try
            val response = peerClient.call(
              leader,
              InternalApi.ReplicaCatchUp,
              ByteWriter()
                .writeString(target.topic)
                .writeInt(target.partition)
                .writeInt(target.followerId)
                .writeInt(target.leaderEpoch)
                .writeInt(config.peerTimeoutMillis)
                .result(),
              config.replicaRecoveryTimeoutMillis
            )
            val error = response.readShort()
            response.ensureFullyRead()
            error
          catch case _: Throwable => Errors.ReplicaNotAvailable

  private def releaseReplicaRecovery(target: ReplicaRecoveryTarget, admitted: Boolean): Boolean =
    if target.leaderId == config.nodeId then
      Option(replicationManager).exists(
        _.completeReplicaRecovery(
          target.topic,
          target.partition,
          target.followerId,
          target.leaderEpoch,
          admitted
        ) == Errors.None
      )
    else
      nodeById.get(target.leaderId).exists { leader =>
        if admitted then synchronizeNode(leader): Unit
        try
          val response = peerClient.call(
            leader,
            InternalApi.ReplicaRecoveryComplete,
            ByteWriter()
              .writeString(target.topic)
              .writeInt(target.partition)
              .writeInt(target.followerId)
              .writeInt(target.leaderEpoch)
              .writeBoolean(admitted)
              .result(),
            config.peerTimeoutMillis
          )
          val error = response.readShort()
          response.ensureFullyRead()
          error == Errors.None
        catch case _: Throwable => false
      }

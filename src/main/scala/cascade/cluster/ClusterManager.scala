package cascade.cluster

import cascade.broker.BrokerConfig
import cascade.protocol.{ByteCursor, ByteWriter, Errors}
import cascade.storage.{CreateTopicResult, TopicRegistry}
import java.util.concurrent.{Callable, ExecutorService, Executors, Future, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable

final case class ClusterCreateResult(errorCode: Short, message: Option[String])
final case class PartitionReassignmentRequest(topic: String, partition: Int, replicas: Option[Vector[Int]])
final case class PartitionReassignmentResult(topic: String, partition: Int, errorCode: Short, message: Option[String])
final case class AlterReassignmentsResult(
    errorCode: Short,
    message: Option[String],
    partitions: Vector[PartitionReassignmentResult]
)
final case class OngoingPartitionReassignment(
    topic: String,
    partition: Int,
    replicas: Vector[Int],
    addingReplicas: Vector[Int],
    removingReplicas: Vector[Int]
)
final case class ListReassignmentsResult(
    errorCode: Short,
    message: Option[String],
    partitions: Vector[OngoingPartitionReassignment]
)
final case class MembershipChangeResult(errorCode: Short, message: Option[String])

private enum ControllerRole:
  case Follower, Candidate, Leader

private final case class MetadataPosition(term: Long, version: Long):
  def atLeast(other: MetadataPosition): Boolean =
    term > other.term || (term == other.term && version >= other.version)

private final case class HeartbeatResult(
    accepted: Boolean,
    responseTerm: Long,
    metadataPosition: MetadataPosition
)

private final case class ReplicaRecoveryTarget(
    topic: String,
    partition: Int,
    leaderId: Int,
    leaderEpoch: Int,
    followerId: Int
)

/** Durable metadata quorum with persisted controller election, broker fencing, and dynamic membership. */
final class ClusterManager(config: BrokerConfig, registry: TopicRegistry, localNode: ClusterNode, peerClient: PeerTransport)
    extends AutoCloseable:
  private val enabled = config.clusterNodes.nonEmpty
  private val bootstrapNodes = if enabled then config.clusterNodes.sortBy(_.id) else Vector(localNode)
  private val bootstrapMembership = Option.when(enabled)(QuorumMembership.bootstrap(bootstrapNodes))
  private val bootstrapNodeById = bootstrapNodes.map(node => node.id -> node).toMap
  private val closed = AtomicBoolean(false)
  private val metadataMutationLock = Object()
  private val peerExecutor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val metadataStore =
    Option.when(enabled)(
      MetadataStore(
        config.dataDirectory.resolve(".cascade").resolve("cluster-metadata.log"),
        config.storageLifecycle.journalCompactionBytes
      )
    )
  private val controllerStore =
    Option.when(enabled)(ControllerStateStore(config.dataDirectory.resolve(".cascade").resolve("controller-state.log")))
  @volatile private var current = metadataStore.map(_.metadata).getOrElse(ClusterMetadata.Empty)

  private val recoveredControllerState = controllerStore.map(_.state).getOrElse(ControllerState.Empty)
  private val initialControllerState =
    if current.controllerTerm > recoveredControllerState.term then
      val updated = ControllerState(current.controllerTerm, None)
      controllerStore.foreach(_.persist(updated))
      updated
    else recoveredControllerState

  @volatile private var currentTerm = initialControllerState.term
  @volatile private var votedFor = initialControllerState.votedFor
  @volatile private var role = if enabled then ControllerRole.Follower else ControllerRole.Leader
  @volatile private var electedControllerId = if enabled then -1 else config.nodeId
  @volatile private var controllerReady = !enabled
  @volatile private var lastControllerContactNanos = if enabled then 0L else System.nanoTime()
  @volatile private var lastQuorumContactNanos = if enabled then 0L else System.nanoTime()
  @volatile private var electionDeadlineNanos = Long.MaxValue
  @volatile private var nextHeartbeatNanos = 0L
  @volatile private var replicationManager: ReplicationManager | Null = null
  @volatile private var coordinatorInstaller: (CoordinatorMetadata => Unit) | Null = null

  private val missedHeartbeats = mutable.HashMap.empty[Int, Int]
  private val pendingRecoveryReleases = mutable.HashMap.empty[ReplicaRecoveryTarget, Boolean]
  private val recoveringNodes = ConcurrentHashMap.newKeySet[Int]()
  private val monitor: Option[ScheduledExecutorService] = Option.when(enabled) {
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-cluster-monitor").factory())
  }

  applyMetadata(current)
  synchronized(resetElectionDeadlineLocked(System.nanoTime(), initial = true))

  def attachReplicationManager(manager: ReplicationManager): Unit = synchronized {
    if replicationManager != null then throw IllegalStateException("replication manager is already attached")
    replicationManager = manager
  }

  def attachCoordinatorInstaller(installer: CoordinatorMetadata => Unit): Unit =
    val initial = synchronized {
      if coordinatorInstaller != null then throw IllegalStateException("coordinator installer is already attached")
      coordinatorInstaller = installer
      current.coordinator
    }
    installer(initial)

  def start(): Unit =
    if enabled && replicationManager == null then throw IllegalStateException("replication manager is not attached")
    monitor.foreach { executor =>
      val interval = math.max(50L, config.controllerHeartbeatMillis.toLong / 2L)
      executor.scheduleWithFixedDelay(
        () =>
          try monitorCluster()
          catch case error: Throwable => System.err.println(s"Cascade cluster monitor failed: ${error.getMessage}"),
        50L,
        interval,
        TimeUnit.MILLISECONDS
      ): Unit
    }

  def isEnabled: Boolean = enabled

  def clusterNodes: Vector[ClusterNode] =
    if enabled then effectiveMembership.currentVoters.map(_.node).sortBy(_.id) else Vector(localNode)

  def controllerNode: Option[ClusterNode] = knownNode(electedControllerId)

  def controllerId: Int = electedControllerId

  def controllerTerm: Long = currentTerm

  def metadataVersion: Long = current.version

  def quorumMembership: QuorumMembership = effectiveMembership

  def coordinatorMetadata: CoordinatorMetadata = current.coordinator

  /** Commits one complete coordinator image, forwarding to the active controller when necessary. */
  def commitCoordinatorState(
      expectedVersion: Long,
      groupState: Vector[Byte],
      deliveryState: Vector[Byte]
  ): Boolean =
    if !enabled then true
    else
      controllerNode match
        case Some(controller) if controller.id == config.nodeId =>
          metadataMutationLock.synchronized {
            commitCoordinatorOnController(expectedVersion, groupState, deliveryState) == Errors.None
          }
        case Some(controller) => forwardCoordinatorCommit(controller, expectedVersion, groupState, deliveryState)
        case None             => false

  def isActiveController: Boolean =
    !enabled || synchronized {
      role == ControllerRole.Leader && electedControllerId == config.nodeId && controllerReady && hasQuorumLeaseLocked()
    }

  def isBrokerFenced: Boolean =
    enabled && synchronized {
      val now = System.nanoTime()
      role match
        case ControllerRole.Leader =>
          !controllerReady || current.controllerTerm != currentTerm || leaseExpired(lastQuorumContactNanos, now)
        case _ =>
          !controllerReady || electedControllerId < 0 || current.controllerTerm != currentTerm ||
            leaseExpired(lastControllerContactNanos, now)
    }

  def topicNames: Vector[String] = if enabled then current.topics.map(_.name).sorted else registry.topicNames

  def topic(name: String): Option[TopicMetadata] = if enabled then current.byName.get(name) else None

  def partition(topic: String, partition: Int): Option[PartitionMetadata] =
    this.topic(topic).flatMap(_.partitions.lift(partition))

  def validateTopic(name: String, partitions: Int, replicationFactor: Int): ClusterCreateResult = synchronized {
    if !registry.validateTopicName(name) then ClusterCreateResult(Errors.InvalidTopic, Some("invalid topic name"))
    else if partitions <= 0 then ClusterCreateResult(Errors.InvalidPartitions, Some("partition count must be positive"))
    else if replicationFactor <= 0 || replicationFactor > clusterNodes.size then
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
    else
      controllerNode match
        case Some(controller) if controller.id == config.nodeId =>
          metadataMutationLock.synchronized(createOnController(name, partitions, replicationFactor))
        case Some(controller) => forwardCreateTopic(controller, name, partitions, replicationFactor)
        case None => ClusterCreateResult(Errors.CoordinatorNotAvailable, Some("controller election is in progress"))

  def alterPartitionReassignments(
      requests: Vector[PartitionReassignmentRequest]
  ): AlterReassignmentsResult = metadataMutationLock.synchronized {
    if !enabled then
      AlterReassignmentsResult(
        Errors.InvalidRequest,
        Some("partition reassignment requires cluster mode"),
        Vector.empty
      )
    else if !isActiveController then
      AlterReassignmentsResult(Errors.NotController, Some("request must be sent to the active controller"), Vector.empty)
    else alterReassignmentsOnController(requests)
  }

  def listPartitionReassignments(
      requested: Option[Set[(String, Int)]]
  ): ListReassignmentsResult = synchronized {
    if !enabled then ListReassignmentsResult(Errors.None, None, Vector.empty)
    else if !isActiveController then
      ListReassignmentsResult(Errors.NotController, Some("request must be sent to the active controller"), Vector.empty)
    else
      val partitions = current.topics.flatMap { topic =>
        topic.partitions.collect {
          case partition
              if partition.isReassigning && requested.forall(_.contains((topic.name, partition.partition))) =>
            OngoingPartitionReassignment(
              topic.name,
              partition.partition,
              partition.replicas,
              partition.addingReplicas,
              partition.removingReplicas
            )
        }
      }
      ListReassignmentsResult(Errors.None, None, partitions)
  }

  def addVoter(voter: QuorumVoter): MembershipChangeResult =
    if !enabled then MembershipChangeResult(Errors.InvalidRequest, Some("voter changes require cluster mode"))
    else
      controllerNode match
        case Some(controller) if controller.id == config.nodeId =>
          metadataMutationLock.synchronized(addVoterOnController(voter))
        case Some(controller) => forwardAddVoter(controller, voter)
        case None => MembershipChangeResult(Errors.NotController, Some("controller election is in progress"))

  def removeVoter(nodeId: Int, directoryId: VoterDirectoryId): MembershipChangeResult =
    if !enabled then MembershipChangeResult(Errors.InvalidRequest, Some("voter changes require cluster mode"))
    else
      controllerNode match
        case Some(controller) if controller.id == config.nodeId =>
          metadataMutationLock.synchronized(removeVoterOnController(nodeId, directoryId))
        case Some(controller) => forwardRemoveVoter(controller, nodeId, directoryId)
        case None => MembershipChangeResult(Errors.NotController, Some("controller election is in progress"))

  def handleInternal(apiKey: Short, cursor: ByteCursor): Array[Byte] = apiKey match
    case InternalApi.Ping =>
      cursor.ensureFullyRead()
      ByteWriter().writeShort(Errors.None).result()
    case InternalApi.ControllerVote => controllerVote(cursor)
    case InternalApi.ControllerHeartbeat => controllerHeartbeat(cursor)
    case InternalApi.AddVoter =>
      val voter = readVoter(cursor)
      cursor.ensureFullyRead()
      val result = metadataMutationLock.synchronized(addVoterOnController(voter))
      ByteWriter().writeShort(result.errorCode).writeNullableString(result.message).result()
    case InternalApi.RemoveVoter =>
      val nodeId = cursor.readInt()
      val directoryId = VoterDirectoryId(cursor.readLong(), cursor.readLong())
      cursor.ensureFullyRead()
      val result = metadataMutationLock.synchronized(removeVoterOnController(nodeId, directoryId))
      ByteWriter().writeShort(result.errorCode).writeNullableString(result.message).result()
    case InternalApi.CoordinatorCommit =>
      val expectedVersion = cursor.readLong()
      val groupState = cursor.readByteArray().toVector
      val deliveryState = cursor.readByteArray().toVector
      cursor.ensureFullyRead()
      val error = metadataMutationLock.synchronized {
        commitCoordinatorOnController(expectedVersion, groupState, deliveryState)
      }
      ByteWriter().writeShort(error).result()
    case InternalApi.MetadataPrepare => metadataPrepare(cursor)
    case InternalApi.MetadataCommit => metadataCommit(cursor)
    case InternalApi.MetadataSnapshot => metadataSnapshot(cursor)
    case InternalApi.CreateTopic =>
      val name = cursor.readString()
      val partitions = cursor.readInt()
      val replicationFactor = cursor.readInt()
      cursor.ensureFullyRead()
      val result =
        if isActiveController then metadataMutationLock.synchronized(createOnController(name, partitions, replicationFactor))
        else ClusterCreateResult(Errors.NotController, Some("metadata mutation must be sent to the elected controller"))
      ByteWriter().writeShort(result.errorCode).writeNullableString(result.message).result()
    case _ => throw IllegalArgumentException(s"unsupported metadata API: $apiKey")

  private def addVoterOnController(voter: QuorumVoter): MembershipChangeResult =
    if !isActiveController then
      MembershipChangeResult(Errors.NotController, Some("request must be sent to the active controller"))
    else
      val membership = effectiveMembership
      if membership.isJoint then
        MembershipChangeResult(Errors.ReassignmentInProgress, Some("another voter change is in progress"))
      else if membership.contains(voter.id) then
        MembershipChangeResult(Errors.DuplicateVoter, Some(s"voter ${voter.id} is already in the quorum"))
      else if membership.voters.exists(existing => existing.node.host == voter.node.host && existing.node.port == voter.node.port) then
        MembershipChangeResult(Errors.InvalidRequest, Some("voter endpoint is already registered"))
      else if !synchronizeNode(voter.node) then
        MembershipChangeResult(Errors.RequestTimedOut, Some("new voter did not synchronize the committed metadata image"))
      else
        val target = (membership.currentVoters :+ voter).sortBy(_.id)
        val joint = membership.beginTransition(target)
        val entered = propose(
          ClusterMetadata(Math.addExact(current.version, 1L), current.topics, currentTerm, Some(joint))
        )
        if !entered then MembershipChangeResult(Errors.RequestTimedOut, Some("metadata quorum did not commit joint membership"))
        else
          val stable = joint.stabilize
          val completed = propose(
            ClusterMetadata(Math.addExact(current.version, 1L), current.topics, currentTerm, Some(stable))
          )
          if completed then MembershipChangeResult(Errors.None, None)
          else MembershipChangeResult(Errors.RequestTimedOut, Some("joint membership committed; stabilization will resume"))

  private def removeVoterOnController(nodeId: Int, directoryId: VoterDirectoryId): MembershipChangeResult =
    if !isActiveController then
      MembershipChangeResult(Errors.NotController, Some("request must be sent to the active controller"))
    else
      val membership = effectiveMembership
      if membership.isJoint then
        MembershipChangeResult(Errors.ReassignmentInProgress, Some("another voter change is in progress"))
      else
        membership.currentVoters.find(_.id == nodeId) match
          case None => MembershipChangeResult(Errors.VoterNotFound, Some(s"voter $nodeId is not in the quorum"))
          case Some(voter) if voter.directoryId != directoryId =>
            MembershipChangeResult(Errors.InvalidVoterKey, Some(s"directory ID does not match voter $nodeId"))
          case Some(_) if membership.currentVoters.size == 1 =>
            MembershipChangeResult(Errors.InvalidRequest, Some("the last voter cannot be removed"))
          case Some(_) if current.topics.exists(_.partitions.exists(_.replicas.contains(nodeId))) =>
            MembershipChangeResult(
              Errors.InvalidReplicaAssignment,
              Some(s"voter $nodeId still hosts partition replicas; reassign them before removal")
            )
          case Some(_) =>
            val target = membership.currentVoters.filterNot(_.id == nodeId)
            val joint = membership.beginTransition(target)
            val entered = propose(
              ClusterMetadata(Math.addExact(current.version, 1L), current.topics, currentTerm, Some(joint))
            )
            if !entered then MembershipChangeResult(Errors.RequestTimedOut, Some("metadata quorum did not commit joint membership"))
            else
              val completed = propose(
                ClusterMetadata(Math.addExact(current.version, 1L), current.topics, currentTerm, Some(joint.stabilize))
              )
              if completed then
                if nodeId == config.nodeId then synchronized(stepDownLocked(currentTerm, None))
                MembershipChangeResult(Errors.None, None)
              else MembershipChangeResult(Errors.RequestTimedOut, Some("joint membership committed; stabilization will resume"))

  private def forwardAddVoter(controller: ClusterNode, voter: QuorumVoter): MembershipChangeResult =
    forwardMembershipChange(
      controller,
      InternalApi.AddVoter,
      writeVoter(ByteWriter(), voter).result()
    )

  private def forwardRemoveVoter(
      controller: ClusterNode,
      nodeId: Int,
      directoryId: VoterDirectoryId
  ): MembershipChangeResult =
    forwardMembershipChange(
      controller,
      InternalApi.RemoveVoter,
      ByteWriter()
        .writeInt(nodeId)
        .writeLong(directoryId.mostSignificantBits)
        .writeLong(directoryId.leastSignificantBits)
        .result()
    )

  private def forwardMembershipChange(
      controller: ClusterNode,
      apiKey: Short,
      payload: Array[Byte]
  ): MembershipChangeResult =
    try
      val response = peerClient.call(controller, apiKey, payload, config.peerTimeoutMillis)
      val result = MembershipChangeResult(response.readShort(), response.readNullableString())
      response.ensureFullyRead()
      result
    catch case error: Throwable => MembershipChangeResult(Errors.NotController, Some(error.getMessage))

  private def writeVoter(writer: ByteWriter, voter: QuorumVoter): ByteWriter =
    writer
      .writeInt(voter.id)
      .writeString(voter.node.host)
      .writeInt(voter.node.port)
      .writeLong(voter.directoryId.mostSignificantBits)
      .writeLong(voter.directoryId.leastSignificantBits)

  private def readVoter(cursor: ByteCursor): QuorumVoter =
    val node = ClusterNode(cursor.readInt(), cursor.readString(), cursor.readInt())
    QuorumVoter(node, VoterDirectoryId(cursor.readLong(), cursor.readLong()))

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      monitor.foreach { executor =>
        executor.shutdownNow(): Unit
        executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
      }
      peerExecutor.shutdownNow(): Unit
      peerExecutor.awaitTermination(5L, TimeUnit.SECONDS): Unit
      controllerStore.foreach(_.close())
      metadataStore.foreach(_.close())

  private def monitorCluster(): Unit =
    if closed.get() then return
    val now = System.nanoTime()
    val currentRole = role
    if currentRole == ControllerRole.Leader then
      if now >= nextHeartbeatNanos then
        nextHeartbeatNanos = now + config.controllerHeartbeatMillis.toLong * 1_000_000L
        val healthy = sendHeartbeats()
        val membership = effectiveMembership
        val acknowledged = healthy ++ Option.when(membership.contains(config.nodeId))(config.nodeId)
        val quorumHealthy = membership.hasQuorum(acknowledged)
        if quorumHealthy then
          synchronized { lastQuorumContactNanos = System.nanoTime() }
          if isActiveController then maintainCluster(healthy)
        else if synchronized(leaseExpired(lastQuorumContactNanos, System.nanoTime())) then
          synchronized(stepDownLocked(currentTerm, None))
    else if now >= electionDeadlineNanos then
      if effectiveMembership.contains(config.nodeId) then startElection()
      else discoverQuorum()

  private def discoverQuorum(): Unit =
    val targets = (effectiveMembership.voters.map(_.node) ++ bootstrapNodes)
      .groupBy(_.id)
      .valuesIterator
      .map(_.last)
      .filterNot(_.id == config.nodeId)
      .toVector
      .sortBy(_.id)
    val discovered = targets.exists(synchronizeFrom)
    synchronized {
      if !discovered then
        electedControllerId = -1
        controllerReady = false
      resetElectionDeadlineLocked(System.nanoTime(), initial = false)
    }

  private def startElection(): Unit =
    val election = synchronized {
      if role == ControllerRole.Leader || System.nanoTime() < electionDeadlineNanos then None
      else if !effectiveMembership.contains(config.nodeId) then
        resetElectionDeadlineLocked(System.nanoTime(), initial = false)
        None
      else
        val term = Math.addExact(currentTerm, 1L)
        persistControllerStateLocked(ControllerState(term, Some(config.nodeId)))
        role = ControllerRole.Candidate
        electedControllerId = -1
        controllerReady = false
        lastControllerContactNanos = 0L
        resetElectionDeadlineLocked(System.nanoTime(), initial = false)
        Some((term, metadataPosition, effectiveMembership))
    }
    election.foreach { case (term, position, membership) =>
      val responses = callPeers(membership.voters.map(_.node).filterNot(_.id == config.nodeId), config.peerTimeoutMillis) { node =>
        val response = peerClient.call(
          node,
          InternalApi.ControllerVote,
          ByteWriter()
            .writeLong(term)
            .writeInt(config.nodeId)
            .writeLong(position.term)
            .writeLong(position.version)
            .result(),
          config.peerTimeoutMillis
        )
        val responseTerm = response.readLong()
        val granted = response.readBoolean()
        response.ensureFullyRead()
        (responseTerm, granted)
      }
      responses.foreach { case (_, (responseTerm, _)) =>
        if responseTerm > currentTerm then synchronized(stepDownLocked(responseTerm, None))
      }
      val votes = responses.collect {
        case (node, (responseTerm, true)) if responseTerm == term => node.id
      }.toSet + config.nodeId
      if membership.hasQuorum(votes) then becomeLeader(term)
    }

  private def becomeLeader(term: Long): Unit =
    val canLead = synchronized {
      if role != ControllerRole.Candidate || currentTerm != term then false
      else
        role = ControllerRole.Leader
        electedControllerId = config.nodeId
        controllerReady = false
        lastControllerContactNanos = System.nanoTime()
        lastQuorumContactNanos = System.nanoTime()
        nextHeartbeatNanos = 0L
        true
    }
    if canLead then
      val established =
        try metadataMutationLock.synchronized {
          val fencedTopics = current.topics.map { topic =>
            topic.copy(partitions = topic.partitions.map { partition =>
              partition.copy(leaderEpoch = Math.addExact(partition.leaderEpoch, 1))
            })
          }
          propose(ClusterMetadata(Math.addExact(current.version, 1L), fencedTopics, term))
        }
        catch
          case error: Throwable =>
            System.err.println(s"Cascade could not establish controller term $term: ${error.getMessage}")
            false
      synchronized {
        if role == ControllerRole.Leader && currentTerm == term then
          if established then controllerReady = true
          else stepDownLocked(term, None)
      }

  private def controllerVote(cursor: ByteCursor): Array[Byte] =
    val term = cursor.readLong()
    val candidateId = cursor.readInt()
    val candidatePosition = MetadataPosition(cursor.readLong(), cursor.readLong())
    cursor.ensureFullyRead()
    val granted = synchronized {
      val membership = effectiveMembership
      if !membership.contains(config.nodeId) || !membership.contains(candidateId) || term < currentTerm then false
      else
        if term > currentTerm then stepDownLocked(term, None)
        val canVote = votedFor.isEmpty || votedFor.contains(candidateId)
        val upToDate = candidatePosition.atLeast(metadataPosition)
        if canVote && upToDate then
          persistControllerStateLocked(ControllerState(currentTerm, Some(candidateId)))
          role = ControllerRole.Follower
          electedControllerId = -1
          controllerReady = false
          lastControllerContactNanos = 0L
          resetElectionDeadlineLocked(System.nanoTime(), initial = false)
          true
        else false
    }
    ByteWriter().writeLong(currentTerm).writeBoolean(granted).result()

  private def controllerHeartbeat(cursor: ByteCursor): Array[Byte] =
    val term = cursor.readLong()
    val leaderId = cursor.readInt()
    val leaderPosition = MetadataPosition(cursor.readLong(), cursor.readLong())
    cursor.ensureFullyRead()
    val accepted = synchronized {
      val accepted = acceptLeaderLocked(term, leaderId)
      if accepted then controllerReady = metadataPosition == leaderPosition
      accepted
    }
    ByteWriter()
      .writeLong(currentTerm)
      .writeShort(if accepted then Errors.None else Errors.NotController)
      .writeLong(current.controllerTerm)
      .writeLong(current.version)
      .result()

  private def metadataPrepare(cursor: ByteCursor): Array[Byte] =
    val term = cursor.readLong()
    val leaderId = cursor.readInt()
    val version = cursor.readLong()
    val metadataTerm = cursor.readLong()
    cursor.ensureFullyRead()
    val accepted = synchronized {
      acceptLeaderLocked(term, leaderId) && metadataTerm == term && version > current.version
    }
    ByteWriter()
      .writeLong(currentTerm)
      .writeShort(if accepted then Errors.None else Errors.InvalidRequest)
      .result()

  private def metadataCommit(cursor: ByteCursor): Array[Byte] =
    val term = cursor.readLong()
    val leaderId = cursor.readInt()
    val metadata = MetadataCodec.decode(cursor.readByteArray())
    cursor.ensureFullyRead()
    val accepted = synchronized {
      if !acceptLeaderLocked(term, leaderId) || metadata.controllerTerm != term then false
      else if metadata.version > current.version then
        commitLocal(metadata)
        controllerReady = true
        true
      else
        val matches = metadata == current
        controllerReady = matches
        matches
    }
    ByteWriter()
      .writeLong(currentTerm)
      .writeShort(if accepted then Errors.None else Errors.InvalidRequest)
      .result()

  private def metadataSnapshot(cursor: ByteCursor): Array[Byte] =
    cursor.ensureFullyRead()
    ByteWriter()
      .writeLong(currentTerm)
      .writeInt(electedControllerId)
      .writeByteArray(MetadataCodec.encode(current))
      .result()

  private def acceptLeaderLocked(term: Long, leaderId: Int): Boolean =
    if !effectiveMembership.contains(leaderId) || term < currentTerm then false
    else
      if term > currentTerm then stepDownLocked(term, Some(leaderId))
      role = ControllerRole.Follower
      electedControllerId = leaderId
      controllerReady = false
      lastControllerContactNanos = System.nanoTime()
      resetElectionDeadlineLocked(lastControllerContactNanos, initial = false)
      true

  private def sendHeartbeats(): Set[Int] =
    val term = currentTerm
    val leaderPosition = metadataPosition
    val responses = callPeers(effectiveMembership.voters.map(_.node).filterNot(_.id == config.nodeId), config.peerTimeoutMillis) { node =>
      val response = peerClient.call(
        node,
        InternalApi.ControllerHeartbeat,
        ByteWriter()
          .writeLong(term)
          .writeInt(config.nodeId)
          .writeLong(leaderPosition.term)
          .writeLong(leaderPosition.version)
          .result(),
        config.peerTimeoutMillis
      )
      val responseTerm = response.readLong()
      val accepted = response.readShort() == Errors.None
      val result = HeartbeatResult(
        accepted = accepted,
        responseTerm = responseTerm,
        metadataPosition = MetadataPosition(response.readLong(), response.readLong())
      )
      response.ensureFullyRead()
      result
    }
    val healthy = mutable.HashSet.empty[Int]
    responses.foreach { case (node, response) =>
      if response.responseTerm > term then
        synchronized(stepDownLocked(response.responseTerm, None))
      else if response.accepted && response.responseTerm == term && synchronized {
          role == ControllerRole.Leader && currentTerm == term
        }
      then
        val livePosition = metadataPosition
        if !livePosition.atLeast(response.metadataPosition) then synchronized(stepDownLocked(term, None))
        else if response.metadataPosition == livePosition then
          healthy += node.id
        else if synchronizeNode(node) then healthy += node.id
    }
    healthy.toSet

  private def maintainCluster(healthy: Set[Int]): Unit =
    finalizeMembershipChange()
    retryPendingRecoveryReleases()
    clusterNodes.foreach { node =>
      val nodeHealthy = node.id == config.nodeId || healthy.contains(node.id)
      val misses = synchronized {
        val value = if nodeHealthy then 0 else missedHeartbeats.getOrElse(node.id, 0) + 1
        missedHeartbeats.update(node.id, value)
        value
      }
      if nodeHealthy && nodeNeedsRecovery(node.id) && (node.id == config.nodeId || synchronizeNode(node)) then
        scheduleNodeRecovery(node.id)
      else if node.id != config.nodeId && misses >= 3 then removeFailedNode(node.id)
    }
    finalizeReassignments()

  private def finalizeMembershipChange(): Unit = metadataMutationLock.synchronized {
    if !isActiveController then return
    val membership = effectiveMembership
    if membership.isJoint then
      val removingLocalController = !membership.targetVoters.exists(_.id == config.nodeId)
      val stable = membership.stabilize
      val completed = propose(
        ClusterMetadata(Math.addExact(current.version, 1L), current.topics, currentTerm, Some(stable))
      )
      if completed && removingLocalController then synchronized(stepDownLocked(currentTerm, None))
  }

  private def scheduleNodeRecovery(nodeId: Int): Unit =
    if recoveringNodes.add(nodeId) then
      try
        peerExecutor.submit(new Runnable:
          override def run(): Unit =
            try recoverNode(nodeId)
            catch case error: Throwable =>
              System.err.println(s"Cascade replica recovery worker failed for node $nodeId: ${error.getMessage}")
            finally recoveringNodes.remove(nodeId): Unit
        ): Unit
      catch
        case error: Throwable =>
          recoveringNodes.remove(nodeId): Unit
          throw error

  private def finalizeReassignments(): Unit = metadataMutationLock.synchronized {
    if !isActiveController then return
    val changedTopics = current.topics.map { topic =>
      topic.copy(partitions = topic.partitions.map { partition =>
        val target = partition.targetReplicas
        if partition.isReassigning && target.nonEmpty && target.forall(partition.inSyncReplicas.contains) then
          val inSync = target.filter(partition.inSyncReplicas.contains)
          val leader = if target.contains(partition.leaderId) then partition.leaderId else inSync.head
          partition.copy(
            leaderId = leader,
            leaderEpoch = Math.addExact(partition.leaderEpoch, 1),
            replicas = target,
            inSyncReplicas = inSync,
            addingReplicas = Vector.empty,
            removingReplicas = Vector.empty
          )
        else partition
      })
    }
    if changedTopics != current.topics then
      val next = ClusterMetadata(Math.addExact(current.version, 1L), changedTopics, currentTerm)
      if !propose(next) then System.err.println("Cascade could not finalize ready partition reassignments")
  }

  private def createOnController(name: String, partitions: Int, replicationFactor: Int): ClusterCreateResult =
    if !isActiveController then
      ClusterCreateResult(Errors.CoordinatorLoadInProgress, Some("controller election or quorum recovery is in progress"))
    else validateTopic(name, partitions, replicationFactor) match
      case error if error.errorCode != Errors.None => error
      case _ =>
        val assignments = Vector.tabulate(partitions) { partition =>
          val brokers = clusterNodes
          val replicas = Vector.tabulate(replicationFactor)(offset => brokers((partition + offset) % brokers.size).id)
          PartitionMetadata(partition, replicas.head, 0, replicas, replicas)
        }
        val next = ClusterMetadata(
          Math.addExact(current.version, 1L),
          (current.topics :+ TopicMetadata(name, assignments)).sortBy(_.name),
          currentTerm
        )
        if propose(next) then ClusterCreateResult(Errors.None, None)
        else ClusterCreateResult(Errors.CoordinatorNotAvailable, Some("metadata quorum is unavailable"))

  private def alterReassignmentsOnController(
      requests: Vector[PartitionReassignmentRequest]
  ): AlterReassignmentsResult =
    val duplicateKeys = requests.groupBy(request => (request.topic, request.partition)).collect {
      case (key, duplicates) if duplicates.size > 1 => key
    }.toSet
    val replacements = mutable.HashMap.empty[(String, Int), PartitionMetadata]
    val results = requests.map { request =>
      val key = (request.topic, request.partition)
      if duplicateKeys.contains(key) then
        PartitionReassignmentResult(
          request.topic,
          request.partition,
          Errors.InvalidRequest,
          Some("partition appears more than once in the request")
        )
      else
        current.byName.get(request.topic).flatMap(_.partitions.lift(request.partition)) match
          case None =>
            PartitionReassignmentResult(
              request.topic,
              request.partition,
              Errors.UnknownTopicOrPartition,
              Some("topic or partition does not exist")
            )
          case Some(partition) =>
            reassignmentReplacement(partition, request.replicas) match
              case Left((error, message)) =>
                PartitionReassignmentResult(request.topic, request.partition, error, Some(message))
              case Right(replacement) =>
                if replacement != partition then replacements.update(key, replacement)
                PartitionReassignmentResult(request.topic, request.partition, Errors.None, None)
    }
    if replacements.isEmpty then AlterReassignmentsResult(Errors.None, None, results)
    else
      val topics = current.topics.map { topic =>
        topic.copy(partitions = topic.partitions.map { partition =>
          replacements.getOrElse((topic.name, partition.partition), partition)
        })
      }
      val committed = propose(ClusterMetadata(Math.addExact(current.version, 1L), topics, currentTerm))
      if committed then AlterReassignmentsResult(Errors.None, None, results)
      else
        AlterReassignmentsResult(
          Errors.RequestTimedOut,
          Some("metadata quorum did not commit the reassignment"),
          Vector.empty
        )

  private def reassignmentReplacement(
      partition: PartitionMetadata,
      requestedReplicas: Option[Vector[Int]]
  ): Either[(Short, String), PartitionMetadata] = requestedReplicas match
    case None =>
      if !partition.isReassigning then
        Left(Errors.NoReassignmentInProgress -> "partition has no reassignment to cancel")
      else
        val original = partition.originalReplicas
        val inSync = original.filter(partition.inSyncReplicas.contains)
        val leader = if original.contains(partition.leaderId) then partition.leaderId else inSync.headOption.getOrElse(-1)
        Right(
          partition.copy(
            leaderId = leader,
            leaderEpoch = Math.addExact(partition.leaderEpoch, 1),
            replicas = original,
            inSyncReplicas = inSync,
            addingReplicas = Vector.empty,
            removingReplicas = Vector.empty
          )
        )
    case Some(target) if target.isEmpty || target.distinct != target || !target.forall(activeNodeIds.contains) =>
      Left(
        Errors.InvalidReplicaAssignment ->
          "replica assignment must be non-empty, unique, and contain only configured broker IDs"
      )
    case Some(target) =>
      val original = if partition.isReassigning then partition.originalReplicas else partition.replicas
      val adding = target.filterNot(original.contains)
      val removing = original.filterNot(target.contains)
      val targetInSync = target.forall(partition.inSyncReplicas.contains)
      if target == partition.replicas && !partition.isReassigning then Right(partition)
      else if adding.isEmpty && targetInSync then
        val inSync = target.filter(partition.inSyncReplicas.contains)
        val leader = if target.contains(partition.leaderId) then partition.leaderId else inSync.head
        Right(
          partition.copy(
            leaderId = leader,
            leaderEpoch = Math.addExact(partition.leaderEpoch, 1),
            replicas = target,
            inSyncReplicas = inSync,
            addingReplicas = Vector.empty,
            removingReplicas = Vector.empty
          )
        )
      else
        val intermediate = (target ++ removing).distinct
        Right(
          partition.copy(
            leaderEpoch = Math.addExact(partition.leaderEpoch, 1),
            replicas = intermediate,
            inSyncReplicas = intermediate.filter(partition.inSyncReplicas.contains),
            addingReplicas = adding,
            removingReplicas = removing
          )
        )

  private def forwardCreateTopic(
      controller: ClusterNode,
      name: String,
      partitions: Int,
      replicationFactor: Int
  ): ClusterCreateResult =
    try
      val response = peerClient.call(
        controller,
        InternalApi.CreateTopic,
        ByteWriter().writeString(name).writeInt(partitions).writeInt(replicationFactor).result(),
        config.peerTimeoutMillis
      )
      val result = ClusterCreateResult(response.readShort(), response.readNullableString())
      response.ensureFullyRead()
      if result.errorCode == Errors.None then synchronizeFrom(controller): Unit
      result
    catch case error: Throwable => ClusterCreateResult(Errors.CoordinatorNotAvailable, Some(error.getMessage))

  private def localCreate(name: String, partitions: Int): ClusterCreateResult =
    registry.createTopic(name, partitions) match
      case CreateTopicResult.Created => ClusterCreateResult(Errors.None, None)
      case CreateTopicResult.AlreadyExists =>
        ClusterCreateResult(Errors.TopicAlreadyExists, Some(s"Topic '$name' already exists"))
      case CreateTopicResult.InvalidPartitions =>
        ClusterCreateResult(Errors.InvalidPartitions, Some("partition count must be positive"))
      case CreateTopicResult.InvalidName => ClusterCreateResult(Errors.InvalidTopic, Some("invalid topic name"))

  private def propose(next: ClusterMetadata): Boolean =
    val coordinator =
      if next.coordinator.version >= current.coordinator.version then next.coordinator else current.coordinator
    val candidate = next.copy(
      membership = next.membership.orElse(current.membership).orElse(bootstrapMembership),
      coordinator = coordinator
    )
    val leadership = synchronized {
      Option.when(
        role == ControllerRole.Leader && electedControllerId == config.nodeId && candidate.controllerTerm == currentTerm &&
          candidate.version == current.version + 1L
      )(currentTerm)
    }
    leadership.exists { term =>
      val quorum =
        val committed = effectiveMembership
        if committed.isJoint then committed
        else candidate.membership.filter(_.isJoint).getOrElse(committed)
      val preparePayload = ByteWriter()
        .writeLong(term)
        .writeInt(config.nodeId)
        .writeLong(candidate.version)
        .writeLong(candidate.controllerTerm)
        .result()
      val prepared = callPeers(quorum.voters.map(_.node).filterNot(_.id == config.nodeId), config.peerTimeoutMillis) { node =>
        val response = peerClient.call(node, InternalApi.MetadataPrepare, preparePayload, config.peerTimeoutMillis)
        val responseTerm = response.readLong()
        val accepted = response.readShort() == Errors.None
        response.ensureFullyRead()
        (responseTerm, accepted)
      }
      prepared.foreach { case (_, (responseTerm, _)) =>
        if responseTerm > term then synchronized(stepDownLocked(responseTerm, None))
      }
      val preparedPeers = prepared.collect {
        case (node, (responseTerm, true)) if responseTerm == term => node
      }
      val preparedNodeIds = preparedPeers.map(_.id).toSet ++ Option.when(quorum.contains(config.nodeId))(config.nodeId)
      if !quorum.hasQuorum(preparedNodeIds) || currentTerm != term || role != ControllerRole.Leader then false
      else
        val commitPayload = ByteWriter()
          .writeLong(term)
          .writeInt(config.nodeId)
          .writeByteArray(MetadataCodec.encode(candidate))
          .result()
        val committed = callPeers(preparedPeers, config.peerTimeoutMillis) { node =>
          val response = peerClient.call(node, InternalApi.MetadataCommit, commitPayload, config.peerTimeoutMillis)
          val responseTerm = response.readLong()
          val accepted = response.readShort() == Errors.None
          response.ensureFullyRead()
          (responseTerm, accepted)
        }
        committed.foreach { case (_, (responseTerm, _)) =>
          if responseTerm > term then synchronized(stepDownLocked(responseTerm, None))
        }
        val committedNodeIds = committed.collect {
          case (node, (responseTerm, true)) if responseTerm == term => node.id
        }.toSet ++ Option.when(quorum.contains(config.nodeId))(config.nodeId)
        if quorum.hasQuorum(committedNodeIds) && synchronized {
            role == ControllerRole.Leader && currentTerm == term
          }
        then
          commitLocal(candidate)
          synchronized { lastQuorumContactNanos = System.nanoTime() }
          true
        else false
    }

  private def commitLocal(metadata: ClusterMetadata): Unit = synchronized {
    metadataStore.foreach(_.commit(metadata))
    current = metadata
    applyMetadata(metadata)
    Option(coordinatorInstaller).foreach(_(metadata.coordinator))
  }

  private def commitCoordinatorOnController(
      expectedVersion: Long,
      groupState: Vector[Byte],
      deliveryState: Vector[Byte]
  ): Short =
    if !isActiveController then Errors.NotController
    else if current.coordinator.version != expectedVersion then Errors.CoordinatorLoadInProgress
    else
      val nextCoordinator = CoordinatorMetadata(
        Math.addExact(expectedVersion, 1L),
        currentTerm,
        groupState,
        deliveryState
      )
      val next = current.copy(
        version = Math.addExact(current.version, 1L),
        controllerTerm = currentTerm,
        coordinator = nextCoordinator
      )
      if propose(next) then Errors.None else Errors.RequestTimedOut

  private def forwardCoordinatorCommit(
      controller: ClusterNode,
      expectedVersion: Long,
      groupState: Vector[Byte],
      deliveryState: Vector[Byte]
  ): Boolean =
    try
      val response = peerClient.call(
        controller,
        InternalApi.CoordinatorCommit,
        ByteWriter()
          .writeLong(expectedVersion)
          .writeByteArray(groupState.toArray)
          .writeByteArray(deliveryState.toArray)
          .result(),
        config.peerTimeoutMillis
      )
      val accepted = response.readShort() == Errors.None
      response.ensureFullyRead()
      accepted && synchronizeFrom(controller)
    catch case _: Throwable => false

  private def applyMetadata(metadata: ClusterMetadata): Unit =
    metadata.topics.foreach { topic =>
      registry.partitions(topic.name) match
        case Some(existing) if existing.size != topic.partitions.size =>
          throw IllegalStateException(
            s"local topic ${topic.name} has ${existing.size} partitions; metadata requires ${topic.partitions.size}"
          )
        case Some(_) => ()
        case None =>
          registry.createTopic(topic.name, topic.partitions.size) match
            case CreateTopicResult.Created | CreateTopicResult.AlreadyExists => ()
            case other => throw IllegalStateException(s"cannot materialize metadata for ${topic.name}: $other")
    }

  private def synchronizeFrom(controller: ClusterNode): Boolean =
    try
      val response = peerClient.call(controller, InternalApi.MetadataSnapshot, Array.emptyByteArray, config.peerTimeoutMillis)
      val responseTerm = response.readLong()
      val leaderId = response.readInt()
      val metadata = MetadataCodec.decode(response.readByteArray())
      response.ensureFullyRead()
      val accepted = synchronized {
        if responseTerm < currentTerm || metadata.controllerTerm > responseTerm then false
        else
          if responseTerm > currentTerm then stepDownLocked(responseTerm, Option.when(leaderId >= 0)(leaderId))
          if leaderId >= 0 then acceptLeaderLocked(responseTerm, leaderId): Unit
          if metadata.version > current.version then commitLocal(metadata)
          val matches = metadata == current
          controllerReady = matches
          matches
      }
      accepted
    catch case _: Throwable => false

  private def synchronizeNode(node: ClusterNode): Boolean =
    val leadership = synchronized {
      Option.when(role == ControllerRole.Leader && electedControllerId == config.nodeId && controllerReady) {
        (currentTerm, current)
      }
    }
    leadership.exists { case (term, metadata) =>
      try
        val response = peerClient.call(
          node,
          InternalApi.MetadataCommit,
          ByteWriter()
            .writeLong(term)
            .writeInt(config.nodeId)
            .writeByteArray(MetadataCodec.encode(metadata))
            .result(),
          config.peerTimeoutMillis
        )
        val responseTerm = response.readLong()
        val accepted = response.readShort() == Errors.None
        response.ensureFullyRead()
        if responseTerm > term then synchronized(stepDownLocked(responseTerm, None))
        accepted && responseTerm == term && synchronized {
          role == ControllerRole.Leader && currentTerm == term && current == metadata
        }
      catch case _: Throwable => false
    }

  private def removeFailedNode(nodeId: Int): Unit = metadataMutationLock.synchronized {
    if !isActiveController then return
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
      val next = ClusterMetadata(Math.addExact(current.version, 1L), changedTopics, currentTerm)
      if !propose(next) then System.err.println(s"Cascade could not commit metadata failover for node $nodeId")
  }

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
    if recovered.nonEmpty then metadataMutationLock.synchronized {
      if !isActiveController then
        recovered.foreach(target => releaseReplicaRecovery(target, admitted = false): Unit)
      else
        val recoveredKeys = recovered.map(target => (target.topic, target.partition) -> target).toMap
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
        val committed =
          changedTopics != current.topics &&
            propose(ClusterMetadata(Math.addExact(current.version, 1L), changedTopics, currentTerm))
        recovered.foreach { target =>
          if releaseReplicaRecovery(target, admitted = committed) then
            synchronized(pendingRecoveryReleases.remove(target): Unit)
          else synchronized(pendingRecoveryReleases.update(target, committed))
        }
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
      knownNode(target.leaderId) match
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
      knownNode(target.leaderId).exists { leader =>
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

  private def persistControllerStateLocked(state: ControllerState): Unit =
    controllerStore.foreach(_.persist(state))
    currentTerm = state.term
    votedFor = state.votedFor

  private def stepDownLocked(term: Long, leaderId: Option[Int]): Unit =
    if term > currentTerm then persistControllerStateLocked(ControllerState(term, None))
    role = ControllerRole.Follower
    electedControllerId = leaderId.filter(effectiveMembership.contains).getOrElse(-1)
    controllerReady = false
    lastControllerContactNanos = Option.when(electedControllerId >= 0)(System.nanoTime()).getOrElse(0L)
    resetElectionDeadlineLocked(System.nanoTime(), initial = false)

  private def resetElectionDeadlineLocked(now: Long, initial: Boolean): Unit =
    val base = config.controllerElectionTimeoutMillis.toLong
    val delayMillis =
      if initial && config.nodeId == config.controllerId then config.controllerHeartbeatMillis.toLong * 2L
      else if initial then
        val rank = effectiveMembership.voters.indexWhere(_.id == config.nodeId).max(0)
        base + rank.toLong * config.controllerHeartbeatMillis.toLong
      else
        val jitterRange = math.max(1L, base / 2L)
        val jitter = Math.floorMod(config.nodeId.toLong * 1_103_515_245L + currentTerm * 12_345L, jitterRange)
        base + jitter
    electionDeadlineNanos = now + delayMillis * 1_000_000L

  private def hasQuorumLeaseLocked(): Boolean = !leaseExpired(lastQuorumContactNanos, System.nanoTime())

  private def leaseExpired(contactNanos: Long, nowNanos: Long): Boolean =
    contactNanos == 0L || nowNanos - contactNanos >= config.controllerElectionTimeoutMillis.toLong * 1_000_000L

  private def metadataPosition: MetadataPosition = MetadataPosition(current.controllerTerm, current.version)

  private def effectiveMembership: QuorumMembership =
    current.membership.orElse(bootstrapMembership).getOrElse(QuorumMembership.bootstrap(Vector(localNode)))

  private def activeNodeIds: Set[Int] = effectiveMembership.currentVoters.map(_.id).toSet

  private def knownNode(nodeId: Int): Option[ClusterNode] =
    effectiveMembership.voters.find(_.id == nodeId).map(_.node).orElse(bootstrapNodeById.get(nodeId))

  private def callPeers[A](
      targets: Vector[ClusterNode],
      timeoutMillis: Int
  )(operation: ClusterNode => A): Vector[(ClusterNode, A)] =
    val futures: Vector[(ClusterNode, Future[Option[A]])] = targets.map { node =>
      node -> peerExecutor.submit(new Callable[Option[A]]:
        override def call(): Option[A] =
          try Some(operation(node))
          catch case _: Throwable => None
      )
    }
    val deadline = System.nanoTime() + timeoutMillis.toLong * 1_000_000L
    futures.flatMap { case (node, future) =>
      val remaining = deadline - System.nanoTime()
      if remaining <= 0L then
        future.cancel(true): Unit
        None
      else
        try future.get(remaining, TimeUnit.NANOSECONDS).map(node -> _)
        catch
          case _: Throwable =>
            future.cancel(true): Unit
            None
    }

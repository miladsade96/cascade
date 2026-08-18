package cascade.cluster

import java.nio.charset.StandardCharsets
import java.util.UUID

final case class ClusterNode(id: Int, host: String, port: Int):
  require(id >= 0, "cluster node ID must be non-negative")
  require(host.nonEmpty, "cluster node host must not be empty")
  require(port > 0 && port <= 65535, "cluster node port must be valid")

object ClusterNode:
  def parse(value: String): ClusterNode =
    val at = value.indexOf('@')
    val colon = value.lastIndexOf(':')
    if at <= 0 || colon <= at + 1 || colon == value.length - 1 then
      throw IllegalArgumentException(s"invalid cluster node '$value'; expected id@host:port")
    ClusterNode(value.substring(0, at).toInt, value.substring(at + 1, colon), value.substring(colon + 1).toInt)

final case class VoterDirectoryId(mostSignificantBits: Long, leastSignificantBits: Long):
  def isZero: Boolean = mostSignificantBits == 0L && leastSignificantBits == 0L

  override def toString: String = UUID(mostSignificantBits, leastSignificantBits).toString

object VoterDirectoryId:
  val Zero: VoterDirectoryId = VoterDirectoryId(0L, 0L)

  def bootstrap(nodeId: Int): VoterDirectoryId =
    val uuid = UUID.nameUUIDFromBytes(s"cascade-voter-$nodeId".getBytes(StandardCharsets.UTF_8))
    VoterDirectoryId(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)

final case class QuorumVoter(node: ClusterNode, directoryId: VoterDirectoryId):
  require(!directoryId.isZero, "voter directory ID must not be zero")

  def id: Int = node.id

object QuorumVoter:
  def bootstrap(node: ClusterNode): QuorumVoter = QuorumVoter(node, VoterDirectoryId.bootstrap(node.id))

final case class QuorumMembership(currentVoters: Vector[QuorumVoter], nextVoters: Vector[QuorumVoter] = Vector.empty):
  require(currentVoters.nonEmpty, "current voter set must not be empty")
  require(currentVoters.map(_.id).distinct.size == currentVoters.size, "current voter IDs must be unique")
  require(nextVoters.map(_.id).distinct.size == nextVoters.size, "next voter IDs must be unique")
  require(
    nextVoters.isEmpty || currentVoters.map(_.id).toSet != nextVoters.map(_.id).toSet,
    "joint voter sets must differ"
  )

  def isJoint: Boolean = nextVoters.nonEmpty

  def voters: Vector[QuorumVoter] =
    (currentVoters ++ nextVoters).groupBy(_.id).valuesIterator.map(_.last).toVector.sortBy(_.id)

  def voterIds: Set[Int] = voters.map(_.id).toSet

  def targetVoters: Vector[QuorumVoter] = if isJoint then nextVoters else currentVoters

  def contains(nodeId: Int): Boolean = voterIds.contains(nodeId)

  def hasQuorum(acknowledgedNodeIds: Set[Int]): Boolean =
    hasMajority(currentVoters, acknowledgedNodeIds) &&
      (!isJoint || hasMajority(nextVoters, acknowledgedNodeIds))

  def beginTransition(target: Vector[QuorumVoter]): QuorumMembership =
    require(!isJoint, "a voter transition is already in progress")
    QuorumMembership(currentVoters, target.sortBy(_.id))

  def stabilize: QuorumMembership =
    require(isJoint, "the voter membership is already stable")
    QuorumMembership(nextVoters.sortBy(_.id))

  private def hasMajority(voters: Vector[QuorumVoter], acknowledgedNodeIds: Set[Int]): Boolean =
    voters.count(voter => acknowledgedNodeIds.contains(voter.id)) >= voters.size / 2 + 1

object QuorumMembership:
  def bootstrap(nodes: Vector[ClusterNode]): QuorumMembership =
    QuorumMembership(nodes.sortBy(_.id).map(QuorumVoter.bootstrap))

final case class PartitionMetadata(
    partition: Int,
    leaderId: Int,
    leaderEpoch: Int,
    replicas: Vector[Int],
    inSyncReplicas: Vector[Int],
    addingReplicas: Vector[Int] = Vector.empty,
    removingReplicas: Vector[Int] = Vector.empty
):
  require(addingReplicas.distinct == addingReplicas, "adding replicas must be unique")
  require(removingReplicas.distinct == removingReplicas, "removing replicas must be unique")
  require(addingReplicas.forall(replicas.contains), "adding replicas must be part of the replica set")
  require(removingReplicas.forall(replicas.contains), "removing replicas must be part of the replica set")
  require(!addingReplicas.exists(removingReplicas.contains), "adding and removing replicas must be disjoint")

  def isReassigning: Boolean = addingReplicas.nonEmpty || removingReplicas.nonEmpty

  def targetReplicas: Vector[Int] = replicas.filterNot(removingReplicas.contains)

  def originalReplicas: Vector[Int] = replicas.filterNot(addingReplicas.contains)

final case class TopicMetadata(name: String, partitions: Vector[PartitionMetadata])

/** One quorum-committed image for every coordinator service hosted by the elected controller. */
final case class CoordinatorMetadata(
    version: Long,
    ownerTerm: Long,
    groupState: Vector[Byte],
    deliveryState: Vector[Byte]
):
  require(version >= 0L, "coordinator version must be non-negative")
  require(ownerTerm >= 0L, "coordinator owner term must be non-negative")

object CoordinatorMetadata:
  val Empty: CoordinatorMetadata = CoordinatorMetadata(0L, 0L, Vector.empty, Vector.empty)

final case class ClusterMetadata(
    version: Long,
    topics: Vector[TopicMetadata],
    controllerTerm: Long = 0L,
    membership: Option[QuorumMembership] = None,
    coordinator: CoordinatorMetadata = CoordinatorMetadata.Empty
):
  require(version >= 0L, "metadata version must be non-negative")
  require(controllerTerm >= 0L, "metadata controller term must be non-negative")
  lazy val byName: Map[String, TopicMetadata] = topics.map(topic => topic.name -> topic).toMap

object ClusterMetadata:
  val Empty: ClusterMetadata = ClusterMetadata(0L, Vector.empty)

object InternalApi:
  val Ping: Short = -100
  val MetadataPrepare: Short = -101
  val MetadataCommit: Short = -102
  val MetadataSnapshot: Short = -103
  val CreateTopic: Short = -104
  val ControllerVote: Short = -105
  val ControllerHeartbeat: Short = -106
  val AddVoter: Short = -107
  val RemoveVoter: Short = -108
  val ReplicaAppend: Short = -110
  val ReplicaCommit: Short = -111
  val ReplicaCatchUp: Short = -112
  val ReplicaReset: Short = -113
  val ReplicaRecoveryComplete: Short = -114
  val ReplicaRecoveryState: Short = -115
  val ReplicaRecoveryProbe: Short = -116
  val ReplicaTruncate: Short = -117

  def contains(apiKey: Short): Boolean = apiKey <= Ping && apiKey >= ReplicaTruncate

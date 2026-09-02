package cascade.cluster

import cascade.storage.TopicLifecyclePolicy
import cascade.coordinator.CoordinatorShard
import java.nio.charset.StandardCharsets
import java.util.UUID
import java.security.MessageDigest

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

final case class TopicMetadata(
    name: String,
    partitions: Vector[PartitionMetadata],
    lifecyclePolicy: Option[TopicLifecyclePolicy] = None
)

final case class TopicConfigResult(errorCode: Short, message: Option[String])

/** Atomic quorum image with independent conflict versions for each virtual coordinator shard. */
final case class CoordinatorMetadata(
    version: Long,
    ownerTerm: Long,
    groupState: Vector[Byte],
    deliveryState: Vector[Byte],
    shardVersions: Vector[Long] = Vector.empty
):
  require(version >= 0L, "coordinator version must be non-negative")
  require(ownerTerm >= 0L, "coordinator owner term must be non-negative")
  require(shardVersions.isEmpty || shardVersions.size == CoordinatorShard.Count, "invalid coordinator shard layout")
  require(shardVersions.forall(_ >= 0L), "shard versions must be non-negative")

  def shardVersion(id: Int): Long =
    require(CoordinatorShard.valid(id), "invalid coordinator shard ID")
    if shardVersions.isEmpty then version else shardVersions(id)

object CoordinatorMetadata:
  val Empty: CoordinatorMetadata = CoordinatorMetadata(0L, 0L, Vector.empty, Vector.empty)

final case class ClusterMetadata(
    version: Long,
    topics: Vector[TopicMetadata],
    controllerTerm: Long = 0L,
    membership: Option[QuorumMembership] = None,
    coordinator: CoordinatorMetadata = CoordinatorMetadata.Empty,
    featureLevels: Map[String, Short] = Map.empty,
    unavailableBrokerIds: Set[Int] = Set.empty
):
  require(version >= 0L, "metadata version must be non-negative")
  require(controllerTerm >= 0L, "metadata controller term must be non-negative")
  require(featureLevels.values.forall(_ > 0), "active feature levels must be positive")
  require(unavailableBrokerIds.forall(_ >= 0), "unavailable broker IDs must be non-negative")
  lazy val byName: Map[String, TopicMetadata] = topics.map(topic => topic.name -> topic).toMap
  lazy val fingerprint: Vector[Byte] = MessageDigest.getInstance("SHA-256").digest(MetadataCodec.encode(this)).toVector

object ClusterMetadata:
  val Empty: ClusterMetadata = ClusterMetadata(0L, Vector.empty)

/** The on-wire/storage capabilities advertised by one broker during a rolling upgrade. */
final case class PeerCapabilities(
    release: String,
    minMetadataFormat: Short,
    maxMetadataFormat: Short,
    featureLevels: Map[String, Short]
):
  require(release.nonEmpty, "peer release must not be empty")
  require(minMetadataFormat > 0, "minimum metadata format must be positive")
  require(maxMetadataFormat >= minMetadataFormat, "maximum metadata format must cover the minimum")
  require(featureLevels.values.forall(_ >= 0), "feature levels must be non-negative")

  def featureLevel(name: String): Short = featureLevels.getOrElse(name, 0.toShort)

object PeerCapabilities:
  /** Existing 1.0.0 brokers answer the versioned ping without a capability body. */
  val Legacy100: PeerCapabilities = PeerCapabilities("1.0.0", 1, 6, Map.empty)

  val Current: PeerCapabilities = PeerCapabilities(
    cascade.BuildInfo.Version,
    MetadataCodec.MinimumReadableFormat,
    MetadataCodec.CurrentFormat,
    Map(
      ClusterFeature.ShardObjectStorage -> 1,
      ClusterFeature.IncrementalCoordinator -> 1,
      ClusterFeature.CoordinatorDeltas -> 1,
      ClusterFeature.CoordinatorSharding -> 1,
      ClusterFeature.CoordinatorFailover -> 1,
      ClusterFeature.ConsumerProtocol -> 1,
      ClusterFeature.OnlineSnapshot -> 1,
      ClusterFeature.AdvancedCompaction -> 1,
      ClusterFeature.DistributedQuotas -> 1
    )
  )

object ClusterFeature:
  val ShardObjectStorage = "shard-object-storage"
  val IncrementalCoordinator = "incremental-coordinator"
  val CoordinatorDeltas = "coordinator-deltas"
  val CoordinatorSharding = "coordinator-sharding"
  val CoordinatorFailover = "coordinator-failover"
  val ConsumerProtocol = "consumer-protocol"
  val OnlineSnapshot = "online-snapshot"
  val AdvancedCompaction = "advanced-compaction"
  val DistributedQuotas = "distributed-quotas"

final case class NegotiatedCapabilities(metadataFormat: Short, featureLevels: Map[String, Short]):
  def featureLevel(name: String): Short = featureLevels.getOrElse(name, 0.toShort)
  def supports(name: String, minimumLevel: Short = 1): Boolean = featureLevel(name) >= minimumLevel

object NegotiatedCapabilities:
  def across(peers: Iterable[PeerCapabilities]): Either[String, NegotiatedCapabilities] =
    val values = peers.toVector
    if values.isEmpty then Left("at least one broker capability is required")
    else
      val minimumWritable = values.map(_.minMetadataFormat).max
      val maximumReadable = values.map(_.maxMetadataFormat).min
      if minimumWritable > maximumReadable then
        Left(s"metadata formats do not overlap: minimum writable $minimumWritable, maximum readable $maximumReadable")
      else
        val featureNames = values.iterator.flatMap(_.featureLevels.keySet).toSet
        val commonFeatures = featureNames.iterator.map { name =>
          name -> values.map(_.featureLevel(name)).min
        }.filter(_._2 > 0).toMap
        Right(NegotiatedCapabilities(maximumReadable, commonFeatures))

object CoordinatorRouting:
  /** Highest-random-weight ownership moves only keys assigned to a node that joins or leaves. */
  def owner(key: String, nodes: Vector[ClusterNode]): Option[ClusterNode] =
    nodes.maxByOption(node => score(key, node.id))

  private def score(key: String, nodeId: Int): BigInt =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(key.getBytes(StandardCharsets.UTF_8))
    digest.update(0.toByte)
    digest.update(java.nio.ByteBuffer.allocate(Integer.BYTES).putInt(nodeId).array())
    BigInt(1, digest.digest().take(8))

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
  val CoordinatorCommit: Short = -109
  val ReplicaAppend: Short = -110
  val ReplicaCommit: Short = -111
  val ReplicaCatchUp: Short = -112
  val ReplicaReset: Short = -113
  val ReplicaRecoveryComplete: Short = -114
  val ReplicaRecoveryState: Short = -115
  val ReplicaRecoveryProbe: Short = -116
  val ReplicaTruncate: Short = -117
  val AlterTopicConfig: Short = -118
  val PeerFeatures: Short = -119
  val CoordinatorDeltaCommit: Short = -120
  val MetadataDeltaCommit: Short = -121

  def contains(apiKey: Short): Boolean = apiKey <= Ping && apiKey >= MetadataDeltaCommit

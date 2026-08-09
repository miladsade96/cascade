package cascade.cluster

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

final case class PartitionMetadata(
    partition: Int,
    leaderId: Int,
    leaderEpoch: Int,
    replicas: Vector[Int],
    inSyncReplicas: Vector[Int]
)

final case class TopicMetadata(name: String, partitions: Vector[PartitionMetadata])

final case class ClusterMetadata(version: Long, topics: Vector[TopicMetadata]):
  lazy val byName: Map[String, TopicMetadata] = topics.map(topic => topic.name -> topic).toMap

object ClusterMetadata:
  val Empty: ClusterMetadata = ClusterMetadata(0L, Vector.empty)

object InternalApi:
  val Ping: Short = -100
  val MetadataPrepare: Short = -101
  val MetadataCommit: Short = -102
  val MetadataSnapshot: Short = -103
  val CreateTopic: Short = -104
  val ReplicaAppend: Short = -110
  val ReplicaCommit: Short = -111
  val ReplicaCatchUp: Short = -112
  val ReplicaReset: Short = -113
  val ReplicaRecoveryComplete: Short = -114

  def contains(apiKey: Short): Boolean = apiKey <= Ping && apiKey >= ReplicaRecoveryComplete

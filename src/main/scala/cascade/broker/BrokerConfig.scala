package cascade.broker

import cascade.cluster.ClusterNode
import cascade.storage.FlushPolicy
import java.nio.file.{Path, Paths}

final case class BrokerConfig(
    bindHost: String = "0.0.0.0",
    port: Int = 9092,
    advertisedHost: String = "localhost",
    advertisedPort: Option[Int] = None,
    dataDirectory: Path = Paths.get("data"),
    maxRequestBytes: Int = 100 * 1024 * 1024,
    segmentBytes: Long = 128L * 1024 * 1024,
    flushPolicy: FlushPolicy = FlushPolicy.Periodic,
    flushIntervalMillis: Long = 1000L,
    flushBytes: Long = 64L * 1024 * 1024,
    nodeId: Int = 1,
    clusterNodes: Vector[ClusterNode] = Vector.empty,
    controllerId: Int = 1,
    defaultReplicationFactor: Int = 1,
    minInSyncReplicas: Int = 1,
    peerTimeoutMillis: Int = 3000,
    autoCreateTopics: Boolean = true
):
  require(port >= 0 && port <= 65535, "port must be between 0 and 65535")
  require(advertisedPort.forall(value => value > 0 && value <= 65535), "advertised port must be valid")
  require(maxRequestBytes >= 1024, "max request size must be at least 1 KiB")
  require(flushIntervalMillis > 0, "flush interval must be positive")
  require(flushBytes > 0, "flush bytes must be positive")
  require(defaultReplicationFactor > 0, "default replication factor must be positive")
  require(minInSyncReplicas > 0, "minimum in-sync replicas must be positive")
  require(peerTimeoutMillis > 0, "peer timeout must be positive")
  require(clusterNodes.map(_.id).distinct.size == clusterNodes.size, "cluster node IDs must be unique")
  require(clusterNodes.isEmpty || clusterNodes.exists(_.id == nodeId), "cluster nodes must contain this node ID")
  require(clusterNodes.isEmpty || clusterNodes.exists(_.id == controllerId), "cluster nodes must contain the controller ID")
  require(
    clusterNodes.isEmpty || defaultReplicationFactor <= clusterNodes.size,
    "default replication factor cannot exceed cluster size"
  )
  require(
    minInSyncReplicas <= defaultReplicationFactor,
    "minimum in-sync replicas cannot exceed default replication factor"
  )

object BrokerConfig:
  def parse(arguments: Array[String]): BrokerConfig =
    @annotation.tailrec
    def loop(remaining: List[String], config: BrokerConfig): BrokerConfig = remaining match
      case Nil => config
      case "--host" :: value :: tail => loop(tail, config.copy(bindHost = value))
      case "--port" :: value :: tail => loop(tail, config.copy(port = value.toInt))
      case "--advertised-host" :: value :: tail => loop(tail, config.copy(advertisedHost = value))
      case "--advertised-port" :: value :: tail => loop(tail, config.copy(advertisedPort = Some(value.toInt)))
      case "--data-dir" :: value :: tail => loop(tail, config.copy(dataDirectory = Paths.get(value)))
      case "--max-request-bytes" :: value :: tail => loop(tail, config.copy(maxRequestBytes = value.toInt))
      case "--segment-bytes" :: value :: tail => loop(tail, config.copy(segmentBytes = value.toLong))
      case "--flush-policy" :: value :: tail => loop(tail, config.copy(flushPolicy = FlushPolicy.parse(value)))
      case "--flush-interval-ms" :: value :: tail => loop(tail, config.copy(flushIntervalMillis = value.toLong))
      case "--flush-bytes" :: value :: tail => loop(tail, config.copy(flushBytes = value.toLong))
      case "--node-id" :: value :: tail => loop(tail, config.copy(nodeId = value.toInt))
      case "--cluster-nodes" :: value :: tail =>
        loop(tail, config.copy(clusterNodes = value.split(',').toVector.map(ClusterNode.parse)))
      case "--controller-id" :: value :: tail => loop(tail, config.copy(controllerId = value.toInt))
      case "--default-replication-factor" :: value :: tail =>
        loop(tail, config.copy(defaultReplicationFactor = value.toInt))
      case "--min-insync-replicas" :: value :: tail => loop(tail, config.copy(minInSyncReplicas = value.toInt))
      case "--peer-timeout-ms" :: value :: tail => loop(tail, config.copy(peerTimeoutMillis = value.toInt))
      case "--no-auto-create" :: tail => loop(tail, config.copy(autoCreateTopics = false))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete option: $option")
    loop(arguments.toList, BrokerConfig())

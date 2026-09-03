package cascade.fault

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.cluster.{ClusterNode, PeerClient}
import cascade.storage.FlushPolicy
import java.net.ServerSocket
import java.nio.file.Files
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

/** Three-or-more broker fixture with deterministic directional peer link control. */
final class FaultCluster(
    size: Int,
    initialVoters: Int = -1,
    defaultReplicationFactor: Int = 3,
    minInSyncReplicas: Int = 2,
    recordCalls: Boolean = true,
    peerTimeoutMillis: Int = 300,
    heartbeatMillis: Int = 100,
    electionTimeoutMillis: Int = 600,
    journalCompactionBytes: Long = 128L * 1024 * 1024,
    maxConnectionsPerIp: Int = 1000,
    offsetBatch: cascade.group.OffsetBatchConfig = cascade.group.OffsetBatchConfig()
) extends AutoCloseable:
  require(size >= 3, "fault cluster requires at least three brokers")
  private val voterCount = if initialVoters < 0 then size else initialVoters
  require(voterCount >= 3 && voterCount <= size, "initial voters must be between three and cluster size")
  private val ports = freePorts(size)
  val nodes: Vector[ClusterNode] = ports.zipWithIndex.map { case (port, index) =>
    ClusterNode(index + 1, "127.0.0.1", port)
  }
  val directories = nodes.map(node => Files.createTempDirectory(s"cascade-fault-${node.id}"))
  val voterNodes: Vector[ClusterNode] = nodes.take(voterCount)
  val faults = NetworkFaultController(if recordCalls then 10000 else 0)
  val configs: Vector[BrokerConfig] = nodes.zip(directories).map { case (node, directory) =>
    BrokerConfig(
      bindHost = node.host,
      port = node.port,
      advertisedHost = node.host,
      advertisedPort = Some(node.port),
      dataDirectory = directory,
      flushPolicy = FlushPolicy.Sync,
      nodeId = node.id,
      clusterNodes = voterNodes,
      controllerId = 1,
      defaultReplicationFactor = defaultReplicationFactor,
      minInSyncReplicas = minInSyncReplicas,
      peerTimeoutMillis = peerTimeoutMillis,
      controllerHeartbeatMillis = heartbeatMillis,
      controllerElectionTimeoutMillis = electionTimeoutMillis,
      offsetBatch = offsetBatch,
      security = cascade.security.BrokerSecurityConfig(resources = cascade.security.ResourceLimits(
        maxConnectionsPerIp = maxConnectionsPerIp)),
      storageLifecycle = cascade.storage.StorageLifecycleConfig(journalCompactionBytes = journalCompactionBytes)
    )
  }
  private val running = mutable.HashMap.empty[Int, KafkaBroker]

  def bootstrapServers: String = nodes.map(node => s"${node.host}:${node.port}").mkString(",")

  def startAll(): Unit = nodes.foreach(node => start(node.id))

  def start(nodeId: Int): KafkaBroker =
    require(!running.contains(nodeId), s"broker $nodeId is already running")
    val broker = KafkaBroker(
      configs(nodeId - 1),
      local => FaultInjectingPeerTransport(local.id, faults, PeerClient())
    )
    broker.start()
    running.update(nodeId, broker)
    broker

  def stop(nodeId: Int): Unit =
    running.remove(nodeId).foreach(_.close())

  def broker(nodeId: Int): KafkaBroker = running(nodeId)

  def runningNodeIds: Set[Int] = running.keySet.toSet

  override def close(): Unit =
    running.values.toVector.foreach(_.close())
    running.clear()
    directories.foreach(deleteTree)

  private def freePorts(count: Int): Vector[Int] =
    val sockets = Vector.fill(count)(ServerSocket(0))
    try sockets.map(_.getLocalPort)
    finally sockets.foreach(_.close())

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

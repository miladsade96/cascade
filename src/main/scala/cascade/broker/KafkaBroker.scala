package cascade.broker

import cascade.cluster.{ClusterManager, ClusterNode, PeerClient, ReplicationManager}
import cascade.group.GroupCoordinator
import cascade.protocol.ProtocolException
import cascade.storage.{FlushStatistics, TopicRegistry}
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.net.{InetSocketAddress, ServerSocket, Socket, SocketException}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

final class KafkaBroker(val config: BrokerConfig) extends AutoCloseable:
  private val running = AtomicBoolean(false)
  private val closed = AtomicBoolean(false)
  private val server = ServerSocket()
  private val connections: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val registry = TopicRegistry(
    config.dataDirectory,
    config.segmentBytes,
    config.flushPolicy,
    config.flushIntervalMillis,
    config.flushBytes
  )
  private val groupCoordinator = GroupCoordinator(config.dataDirectory.resolve(".cascade").resolve("consumer-offsets.log"))
  @volatile private var acceptThread: Thread | Null = null
  @volatile private var handler: RequestHandler | Null = null
  @volatile private var clusterManager: ClusterManager | Null = null
  @volatile private var replicationManager: ReplicationManager | Null = null
  @volatile private var peerClient: PeerClient | Null = null

  def start(): Unit = synchronized {
    if closed.get() then throw IllegalStateException("broker is closed")
    if running.get() then throw IllegalStateException("broker is already running")
    server.setReuseAddress(true)
    server.bind(InetSocketAddress(config.bindHost, config.port))
    val localNode = config.clusterNodes.find(_.id == config.nodeId).getOrElse {
      ClusterNode(config.nodeId, config.advertisedHost, advertisedPort)
    }
    val peers = PeerClient()
    val cluster = ClusterManager(config, registry, localNode, peers)
    val replication = ReplicationManager(config, cluster, registry, peers)
    peerClient = peers
    clusterManager = cluster
    replicationManager = replication
    handler = RequestHandler(config, registry, groupCoordinator, cluster, replication, advertisedPort)
    running.set(true)
    acceptThread = Thread.ofPlatform().name("cascade-acceptor").start(() => acceptLoop())
    cluster.start()
  }

  def boundPort: Int = server.getLocalPort

  def advertisedPort: Int = config.advertisedPort.getOrElse(boundPort)

  def bootstrapServers: String = s"${config.advertisedHost}:$advertisedPort"

  def flushStatistics: FlushStatistics = registry.flushStatistics

  override def close(): Unit = synchronized {
    if closed.compareAndSet(false, true) then
      running.set(false)
      server.close()
      connections.shutdownNow()
      Option(acceptThread).foreach(_.join(5000))
      connections.awaitTermination(5, TimeUnit.SECONDS)
      Option(replicationManager).foreach(_.close())
      Option(clusterManager).foreach(_.close())
      Option(peerClient).foreach(_.close())
      groupCoordinator.close()
      registry.close()
  }

  private def acceptLoop(): Unit =
    while running.get() do
      try
        val socket = server.accept()
        socket.setTcpNoDelay(true)
        socket.setKeepAlive(true)
        val task = new Runnable:
          override def run(): Unit = serve(socket)
        connections.submit(task): Unit
      catch
        case _: SocketException if !running.get() => ()
        case error: Throwable =>
          System.err.println(s"Cascade accept error: ${error.getMessage}")

  private def serve(socket: Socket): Unit =
    try
      val input = DataInputStream(BufferedInputStream(socket.getInputStream, 64 * 1024))
      val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream, 64 * 1024))
      var connected = true
      while connected && running.get() do
        try
          val size = input.readInt()
          if size <= 0 || size > config.maxRequestBytes then
            throw ProtocolException(s"invalid request frame size: $size")
          val frame = new Array[Byte](size)
          input.readFully(frame)
          val currentHandler = handler
          if currentHandler == null then throw IllegalStateException("request handler is not initialized")
          currentHandler.handle(frame).foreach { response =>
            output.write(response)
            output.flush()
          }
        catch
          case _: EOFException => connected = false
    catch
      case _: SocketException => ()
      case error: ProtocolException => System.err.println(s"Cascade protocol error: ${error.getMessage}")
      case error: Throwable => System.err.println(s"Cascade connection error: ${error.getMessage}")
    finally socket.close()

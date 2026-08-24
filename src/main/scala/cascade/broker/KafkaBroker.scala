package cascade.broker

import cascade.cluster.{ClusterManager, ClusterNode, PeerClient, PeerTransport, ReplicationManager}
import cascade.coordinator.CoordinatorStateMachine
import cascade.delivery.DeliveryCoordinator
import cascade.group.GroupCoordinator
import cascade.protocol.ProtocolException
import cascade.security.{ConnectionAdmission, ConnectionAdmissionSnapshot, ConnectionSession, TlsClientAuth, TlsContextFactory}
import cascade.storage.{FlushStatistics, TopicRegistry}
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.net.{InetSocketAddress, ServerSocket, Socket, SocketException}
import javax.net.ssl.SSLServerSocket
import javax.net.ssl.{SSLPeerUnverifiedException, SSLSocket}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

final class KafkaBroker(
    val config: BrokerConfig,
    peerTransportFactory: ClusterNode => PeerTransport = _ => PeerClient()
) extends AutoCloseable:
  private val running = AtomicBoolean(false)
  private val closed = AtomicBoolean(false)
  private val shutdownMarker = ShutdownMarker(config.dataDirectory)
  val recoveryMode: RecoveryMode = shutdownMarker.beginRecovery()
  private val server: ServerSocket =
    if config.security.protocol.tls then TlsContextFactory.create(config.security.tls).getServerSocketFactory.createServerSocket()
    else ServerSocket()
  private val connections: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val connectionAdmission = ConnectionAdmission(
    config.security.resources.maxConnections,
    config.security.resources.maxConnectionsPerIp
  )
  private val registry = TopicRegistry(
    config.dataDirectory,
    config.segmentBytes,
    config.flushPolicy,
    config.flushIntervalMillis,
    config.flushBytes,
    config.storageLifecycle
  )
  private val coordinatorLock = Object()
  private val clustered = config.clusterNodes.nonEmpty
  private val groupCoordinator = GroupCoordinator(
    config.dataDirectory.resolve(".cascade").resolve("consumer-offsets.log"),
    coordinatorLock,
    durableLocal = !clustered,
    scheduleExpiration = !clustered,
    offsetRetentionMillis = config.storageLifecycle.offsetRetentionMillis,
    journalCompactionBytes = config.storageLifecycle.journalCompactionBytes
  )
  @volatile private var acceptThread: Thread | Null = null
  @volatile private var handler: RequestHandler | Null = null
  @volatile private var clusterManager: ClusterManager | Null = null
  @volatile private var replicationManager: ReplicationManager | Null = null
  @volatile private var deliveryCoordinator: DeliveryCoordinator | Null = null
  @volatile private var coordinatorStateMachine: CoordinatorStateMachine | Null = null
  @volatile private var peerClient: PeerTransport | Null = null

  def start(): Unit = synchronized {
    if closed.get() then throw IllegalStateException("broker is closed")
    if running.get() then throw IllegalStateException("broker is already running")
    server.setReuseAddress(true)
    server match
      case tlsServer: SSLServerSocket =>
        tlsServer.setEnabledProtocols(config.security.tls.enabledProtocols.toArray)
        config.security.tls.clientAuth match
          case TlsClientAuth.None      => ()
          case TlsClientAuth.Requested => tlsServer.setWantClientAuth(true)
          case TlsClientAuth.Required  => tlsServer.setNeedClientAuth(true)
      case _ => ()
    server.bind(InetSocketAddress(config.bindHost, config.port))
    val localNode = ClusterNode(config.nodeId, config.advertisedHost, advertisedPort)
    val peers = peerTransportFactory(localNode)
    val cluster = ClusterManager(config, registry, localNode, peers)
    val replication = ReplicationManager(config, cluster, registry, peers)
    cluster.attachReplicationManager(replication)
    val delivery = DeliveryCoordinator(
      config.dataDirectory.resolve(".cascade").resolve("delivery-state.log"),
      registry,
      groupCoordinator,
      coordinatorLock,
      durableLocal = !clustered,
      scheduleExpiration = !clustered,
      journalCompactionBytes = config.storageLifecycle.journalCompactionBytes
    )
    val coordinatorState = Option.when(clustered)(CoordinatorStateMachine(cluster, groupCoordinator, delivery, coordinatorLock))
    peerClient = peers
    clusterManager = cluster
    replicationManager = replication
    deliveryCoordinator = delivery
    coordinatorStateMachine = coordinatorState.orNull
    handler = RequestHandler(config, registry, groupCoordinator, cluster, replication, delivery, advertisedPort)
    running.set(true)
    acceptThread = Thread.ofPlatform().name("cascade-acceptor").start(() => acceptLoop())
    cluster.start()
  }

  def boundPort: Int = server.getLocalPort

  def advertisedPort: Int = config.advertisedPort.getOrElse(boundPort)

  def bootstrapServers: String = s"${config.advertisedHost}:$advertisedPort"

  def flushStatistics: FlushStatistics = registry.flushStatistics

  def lifecycleStatistics: cascade.storage.LifecycleStatistics = registry.lifecycleStatistics

  def connectionAdmissionSnapshot: ConnectionAdmissionSnapshot = connectionAdmission.snapshot

  override def close(): Unit = synchronized {
    if closed.compareAndSet(false, true) then
      running.set(false)
      server.close()
      connections.shutdownNow()
      Option(acceptThread).foreach(_.join(5000))
      connections.awaitTermination(5, TimeUnit.SECONDS)
      Option(handler).foreach(_.close())
      Option(coordinatorStateMachine).foreach(_.close())
      Option(replicationManager).foreach(_.close())
      Option(clusterManager).foreach(_.close())
      Option(peerClient).foreach(_.close())
      Option(deliveryCoordinator).foreach(_.close())
      groupCoordinator.close()
      registry.close()
      shutdownMarker.markClean()
  }

  private def acceptLoop(): Unit =
    while running.get() do
      try
        val socket = server.accept()
        val remoteAddress = socket.getInetAddress.getHostAddress
        connectionAdmission.tryAcquire(remoteAddress) match
          case None => socket.close()
          case Some(lease) =>
            try
              socket.setTcpNoDelay(true)
              socket.setKeepAlive(true)
              val task = new Runnable:
                override def run(): Unit =
                  try serve(socket)
                  finally lease.close()
              connections.submit(task): Unit
            catch
              case error: Throwable =>
                lease.close()
                socket.close()
                throw error
      catch
        case _: SocketException if !running.get() => ()
        case error: Throwable =>
          System.err.println(s"Cascade accept error: ${error.getMessage}")

  private def serve(socket: Socket): Unit =
    try
      val session = connectionSession(socket)
      Option(handler).foreach(_.auditTransport(session))
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
          currentHandler.handle(frame, session).foreach { response =>
            output.write(response)
            output.flush()
          }
          if session.terminateRequested then connected = false
        catch
          case _: EOFException => connected = false
    catch
      case _: SocketException => ()
      case error: ProtocolException => System.err.println(s"Cascade protocol error: ${error.getMessage}")
      case error: Throwable => System.err.println(s"Cascade connection error: ${error.getMessage}")
    finally socket.close()

  private def connectionSession(socket: Socket): ConnectionSession =
    val transportPrincipal = socket match
      case tlsSocket: SSLSocket =>
        tlsSocket.startHandshake()
        try Some(tlsSocket.getSession.getPeerPrincipal.getName)
        catch case _: SSLPeerUnverifiedException => None
      case _ => None
    ConnectionSession(
      socket.getInetAddress.getHostAddress,
      secure = socket.isInstanceOf[SSLSocket],
      authenticationRequired = config.security.protocol.sasl,
      transportPrincipal = transportPrincipal
    )

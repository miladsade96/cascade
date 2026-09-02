package cascade.broker

import cascade.cluster.{ClusterManager, ClusterNode, PeerClient, PeerTransport, ReplicationManager}
import cascade.coordinator.CoordinatorStateMachine
import cascade.delivery.DeliveryCoordinator
import cascade.group.GroupCoordinator
import cascade.operations.{AuthenticationMetrics, BrokerHealth, BrokerMetricsSnapshot, CapacityLimits, CapacityMonitor, HealthPolicy, OperationsServer, PeerSecurityMetrics, StructuredLogger, TrafficMetrics, TrafficQuotaSnapshot}
import cascade.protocol.{ApiKey, ProtocolException, ProtocolThrottle}
import cascade.security.{ConnectionAdmission, ConnectionAdmissionSnapshot, ConnectionSession, QuotaDecision, ReloadableTlsContext, RequestAdmission, RequestAdmissionSnapshot, RequestQuota, RequestQuotaSnapshot, TlsClientAuth}
import cascade.storage.{FlushStatistics, TopicRegistry}
import cascade.backup.{BackupCreator, BackupManifest}
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream, EOFException}
import java.net.{InetSocketAddress, ServerSocket, Socket, SocketException}
import java.nio.file.Files
import javax.net.ssl.{SSLPeerUnverifiedException, SSLSocket}
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock
import java.nio.file.Path

final class KafkaBroker(
    val config: BrokerConfig,
    peerTransportFactory: (ClusterNode, Option[ReloadableTlsContext]) => PeerTransport
) extends AutoCloseable:
  def this(config: BrokerConfig, peerTransportFactory: ClusterNode => PeerTransport) =
    this(config, (node, _) => peerTransportFactory(node))

  def this(config: BrokerConfig) =
    this(
      config,
      (_, tlsContext) => PeerClient(
        localNodeId = if config.security.peer.protocol.tls then config.nodeId else -1,
        security = config.security.peer,
        tls = Option.when(config.security.peer.protocol.tls)(config.security.tls),
        tlsContext = tlsContext
      )
    )

  config.security.validate(): Unit
  config.operations.validate(): Unit
  private val running = AtomicBoolean(false)
  private val closed = AtomicBoolean(false)
  private val eventLog = StructuredLogger.from(config.operations)
  private val operationalEventsEnabled = config.operations.enabled || config.operations.structuredLog.nonEmpty
  private val trafficMetrics = TrafficMetrics()
  private val peerSecurityMetrics = PeerSecurityMetrics()
  private val authenticationMetrics = AuthenticationMetrics()
  private val startedAtNanos = AtomicLong(0L)
  private val shutdownMarker = ShutdownMarker(config.dataDirectory)
  val recoveryMode: RecoveryMode = shutdownMarker.beginRecovery()
  private val tlsContext = Option.when(config.security.protocol.tls) {
    ReloadableTlsContext(
      config.security.tls,
      snapshot =>
        if operationalEventsEnabled then
          eventLog.info("tls_material_reloaded", brokerFields ++ Map("generation" -> snapshot.generation.toString)),
      error =>
        if operationalEventsEnabled then eventLog.error("tls_material_reload_failed", error, brokerFields)
    )
  }
  private val server = ServerSocket()
  private val connections: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val connectionAdmission = ConnectionAdmission(
    config.security.resources.maxConnections,
    config.security.resources.maxConnectionsPerIp
  )
  private val requestAdmission = RequestAdmission(config.security.resources.maxInFlightRequests)
  private val snapshotBarrier = ReentrantReadWriteLock(true)
  private val requestQuota = RequestQuota(
    config.security.resources.requestBytesPerSecond,
    config.security.resources.requestBurstBytes,
    config.security.resources.maxThrottleMillis,
    clusterShareCount = () => quotaShareCount
  )
  private val responseQuota = RequestQuota(
    config.security.resources.responseBytesPerSecond,
    config.security.resources.responseBurstBytes,
    config.security.resources.maxThrottleMillis,
    clusterShareCount = () => quotaShareCount
  )
  private val produceQuota = RequestQuota(
    config.security.resources.produceBytesPerSecond,
    config.security.resources.produceBurstBytes,
    config.security.resources.maxThrottleMillis,
    clusterShareCount = () => quotaShareCount
  )
  private val fetchQuota = RequestQuota(
    config.security.resources.fetchBytesPerSecond,
    config.security.resources.fetchBurstBytes,
    config.security.resources.maxThrottleMillis,
    clusterShareCount = () => quotaShareCount
  )
  private val registry = TopicRegistry(
    config.dataDirectory,
    config.segmentBytes,
    config.flushPolicy,
    config.flushIntervalMillis,
    config.flushBytes,
    config.storageLifecycle,
    (event, error) => eventLog.error(event, error, brokerFields)
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
  @volatile private var operationsServer: OperationsServer | Null = null
  @volatile private var capacityMonitor: CapacityMonitor | Null = null

  def start(): Unit = synchronized {
    if closed.get() then throw IllegalStateException("broker is closed")
    if running.get() then throw IllegalStateException("broker is already running")
    startedAtNanos.set(System.nanoTime())
    if operationalEventsEnabled then
      eventLog.info("broker_starting", brokerFields ++ Map("recovery_mode" -> recoveryMode.toString.toLowerCase))
    server.setReuseAddress(true)
    server.bind(InetSocketAddress(config.bindHost, config.port))
    val localNode = ClusterNode(config.nodeId, config.advertisedHost, advertisedPort)
    val peers = peerTransportFactory(localNode, tlsContext)
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
    handler = RequestHandler(
      config,
      registry,
      groupCoordinator,
      cluster,
      replication,
      delivery,
      advertisedPort,
      peerSecurityMetrics,
      authenticationMetrics
    )
    val operations = config.operations.port.map { _ =>
      OperationsServer(
        config.operations,
        () => metricsSnapshot,
        () => healthSnapshot,
        error => eventLog.error("operations_server_error", error, brokerFields)
      )
    }
    operationsServer = operations.orNull
    val capacity = Option.when(operationalEventsEnabled) {
      CapacityMonitor(
        config.operations.capacityAlerts,
        () => metricsSnapshot,
        CapacityLimits(config.security.resources.maxConnections, config.security.resources.maxInFlightRequests),
        alert => eventLog.warn("capacity_alert", brokerFields ++ alert.fields),
        code => eventLog.info("capacity_alert_resolved", brokerFields ++ Map("alert" -> code)),
        error => eventLog.error("capacity_monitor_error", error, brokerFields)
      )
    }
    capacityMonitor = capacity.orNull
    running.set(true)
    acceptThread = Thread.ofPlatform().name("cascade-acceptor").start(() => acceptLoop())
    cluster.start()
    operations.foreach(_.start())
    capacity.foreach(_.start())
    if operationalEventsEnabled then
      eventLog.info(
        "broker_started",
        brokerFields ++ Map(
          "kafka_port" -> boundPort.toString,
          "operations_port" -> operations.map(_.boundPort.toString).getOrElse("disabled"),
          "security_protocol" -> config.security.protocol.toString
        )
      )
  }

  def boundPort: Int = server.getLocalPort

  def advertisedPort: Int = config.advertisedPort.getOrElse(boundPort)

  def bootstrapServers: String = s"${config.advertisedHost}:$advertisedPort"

  def operationsPort: Option[Int] = Option(operationsServer).map(_.boundPort)

  def flushStatistics: FlushStatistics = registry.flushStatistics

  def lifecycleStatistics: cascade.storage.LifecycleStatistics = registry.lifecycleStatistics

  def connectionAdmissionSnapshot: ConnectionAdmissionSnapshot = connectionAdmission.snapshot

  def requestAdmissionSnapshot: RequestAdmissionSnapshot = requestAdmission.snapshot

  def requestQuotaSnapshot: RequestQuotaSnapshot = requestQuota.snapshot

  def responseQuotaSnapshot: RequestQuotaSnapshot = responseQuota.snapshot

  def produceQuotaSnapshot: RequestQuotaSnapshot = produceQuota.snapshot

  def fetchQuotaSnapshot: RequestQuotaSnapshot = fetchQuota.snapshot

  /** Creates a checksummed, restore-compatible snapshot while the broker remains online. */
  def createOnlineSnapshot(targetDirectory: Path): BackupManifest =
    if !running.get() then throw IllegalStateException("broker must be running for an online snapshot")
    val cluster = Option(clusterManager).getOrElse(throw IllegalStateException("cluster manager is not initialized"))
    if !cluster.supportsFeature(cascade.cluster.ClusterFeature.OnlineSnapshot) then
      throw IllegalStateException("online snapshots require every voter to advertise the feature")
    val lock = snapshotBarrier.writeLock()
    lock.lock()
    try registry.withSnapshotBarrier(_ => BackupCreator.createOnline(config.dataDirectory, targetDirectory))
    finally lock.unlock()

  def metricsSnapshot: BrokerMetricsSnapshot =
    val cluster = Option(clusterManager)
    val topicNames = try cluster.map(_.topicNames).getOrElse(registry.topicNames) catch case _: Throwable => Vector.empty
    val localPartitions = try topicNames.flatMap(name => registry.partitions(name).toVector.flatten).size catch case _: Throwable => 0
    val connections = connectionAdmission.snapshot
    val requests = requestAdmission.snapshot
    val quota = requestQuota.snapshot
    val flush = registry.flushStatistics
    val lifecycle = registry.lifecycleStatistics
    val fileStore = Files.getFileStore(config.dataDirectory)
    val runtime = Runtime.getRuntime
    val start = startedAtNanos.get()
    BrokerMetricsSnapshot(
      nodeId = config.nodeId,
      uptimeMillis = if start == 0L then 0L else math.max(0L, (System.nanoTime() - start) / 1_000_000L),
      running = running.get(),
      clustered = clustered,
      controllerId = cluster.map(_.controllerId).getOrElse(config.nodeId),
      brokerFenced = cluster.exists(_.isBrokerFenced),
      topics = topicNames.size,
      partitions = localPartitions,
      activeConnections = connections.active,
      rejectedConnections = connections.rejected,
      activeRequests = requests.active,
      rejectedRequests = requests.rejected,
      quotaPrincipals = quota.principals,
      quotaThrottledRequests = quota.throttled,
      quotaRejectedRequests = quota.rejected,
      quotaThrottleMillis = quota.throttleMillis,
      traffic = trafficMetrics.snapshot,
      flushOperations = flush.forces,
      flushBytes = flush.bytes,
      flushNanos = flush.nanos,
      pendingFlushBytes = flush.pendingBytes,
      lifecycleRuns = lifecycle.runs,
      retiredSegments = lifecycle.retiredSegments,
      reclaimedBytes = lifecycle.reclaimedBytes,
      rejectedAppends = lifecycle.rejectedAppends,
      usableDiskBytes = fileStore.getUsableSpace,
      totalDiskBytes = fileStore.getTotalSpace,
      heapUsedBytes = runtime.totalMemory() - runtime.freeMemory(),
      heapMaxBytes = runtime.maxMemory(),
      peerSecurity = peerSecurityMetrics.snapshot,
      authentication = authenticationMetrics.snapshot,
      tlsReload = tlsContext.map(_.snapshot).getOrElse(cascade.security.TlsReloadSnapshot.Empty),
      trafficQuotas = TrafficQuotaSnapshot(requestQuota.snapshot, responseQuota.snapshot, produceQuota.snapshot, fetchQuota.snapshot),
      coordinator = Option(coordinatorStateMachine).map(_.metricsSnapshot).getOrElse(cascade.coordinator.CoordinatorMetricsSnapshot.Empty)
    )

  def healthSnapshot: BrokerHealth =
    BrokerHealth.evaluate(
      metricsSnapshot,
      HealthPolicy(
        config.operations.readinessMaxPendingFlushBytes,
        math.max(config.storageLifecycle.minimumFreeBytes, config.operations.capacityAlerts.minimumFreeBytes)
      ),
      eventLog.lastFailure,
      Option(handler).flatMap(_.peerIdentityReloadError),
      Option(handler).flatMap(_.credentialReloadError),
      tlsContext.flatMap(_.lastReloadError)
    )

  override def close(): Unit = synchronized {
    if closed.compareAndSet(false, true) then
      if operationalEventsEnabled then eventLog.info("broker_stopping", brokerFields)
      running.set(false)
      Option(capacityMonitor).foreach(_.close())
      Option(operationsServer).foreach(_.close())
      server.close()
      connections.shutdownNow()
      Option(acceptThread).foreach(_.join(5000))
      connections.awaitTermination(5, TimeUnit.SECONDS)
      Option(handler).foreach(_.close())
      Option(coordinatorStateMachine).foreach(_.close())
      Option(replicationManager).foreach(_.close())
      Option(clusterManager).foreach(_.close())
      Option(peerClient).foreach(_.close())
      tlsContext.foreach(_.close())
      Option(deliveryCoordinator).foreach(_.close())
      groupCoordinator.close()
      registry.close()
      shutdownMarker.markClean()
      if operationalEventsEnabled then eventLog.info("broker_stopped", brokerFields)
      eventLog.close()
  }

  private def acceptLoop(): Unit =
    while running.get() do
      try
        val socket = secureClientSocket(server.accept())
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
          eventLog.error("connection_accept_error", error, brokerFields)

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
          val apiKey = requestApiKey(frame)
          connected = applyIngressQuota(requestQuota, session, size + Integer.BYTES) &&
            (apiKey != ApiKey.Produce || applyIngressQuota(produceQuota, session, size + Integer.BYTES)) &&
            handleAdmitted(frame, apiKey, session, output)
        catch
          case _: EOFException => connected = false
    catch
      case _: SocketException => ()
      case error: ProtocolException => eventLog.error("protocol_error", error, brokerFields)
      case error: Throwable => eventLog.error("connection_error", error, brokerFields)
    finally socket.close()

  private def handleAdmitted(
      frame: Array[Byte],
      apiKey: Short,
      session: ConnectionSession,
      output: DataOutputStream
  ): Boolean =
    requestAdmission.tryAcquire() match
      case None => false
      case Some(lease) =>
        val started = System.nanoTime()
        trafficMetrics.recordRequest(frame.length + Integer.BYTES)
        try
          val currentHandler = handler
          if currentHandler == null then throw IllegalStateException("request handler is not initialized")
          val barrier = snapshotBarrier.readLock()
          barrier.lock()
          try currentHandler.handle(frame, session).foreach { response =>
            val ingressDelay = session.consumeThrottleMillis().toLong
            val responseDelay = egressDelay(responseQuota, session.principal, response.length)
            val fetchDelay = if apiKey == ApiKey.Fetch then egressDelay(fetchQuota, session.principal, response.length) else 0L
            val egressThrottle = Math.addExact(responseDelay, fetchDelay)
            ProtocolThrottle.add(response, apiKey, Math.addExact(ingressDelay, egressThrottle))
            if egressThrottle > 0L then Thread.sleep(egressThrottle)
            output.write(response)
            output.flush()
            trafficMetrics.recordResponse(response.length)
          }
          finally barrier.unlock()
          !session.terminateRequested
        catch
          case error: Throwable =>
            trafficMetrics.recordFailure()
            throw error
        finally
          trafficMetrics.recordDuration(System.nanoTime() - started)
          lease.close()

  private def applyIngressQuota(quota: RequestQuota, session: ConnectionSession, bytes: Int): Boolean =
    quota.evaluate(session.principal, bytes) match
      case QuotaDecision.Rejected(_) => false
      case QuotaDecision.Throttle(delayMillis) =>
        session.addThrottle(delayMillis)
        if delayMillis > 0L then Thread.sleep(delayMillis)
        true
      case QuotaDecision.Allowed => true

  private def egressDelay(quota: RequestQuota, principal: String, bytes: Int): Long =
    quota.evaluate(principal, bytes, rejectExcess = false) match
      case QuotaDecision.Throttle(delayMillis) => delayMillis
      case QuotaDecision.Allowed               => 0L
      case QuotaDecision.Rejected(_)           => throw IllegalStateException("egress quota unexpectedly rejected traffic")

  private def quotaShareCount: Int =
    Option(clusterManager).map(_.clusterNodes.size).getOrElse(math.max(1, config.clusterNodes.size))

  private def requestApiKey(frame: Array[Byte]): Short =
    if frame.length < 2 then -1.toShort
    else (((frame(0) & 0xff) << 8) | (frame(1) & 0xff)).toShort

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

  private def secureClientSocket(socket: Socket): Socket =
    tlsContext match
      case None => socket
      case Some(reloader) =>
        try
          val context = reloader.current.context
          val secure = context.getSocketFactory
            .createSocket(socket, socket.getInetAddress.getHostAddress, socket.getPort, true)
            .asInstanceOf[SSLSocket]
          secure.setUseClientMode(false)
          secure.setEnabledProtocols(config.security.tls.enabledProtocols.toArray)
          config.security.tls.clientAuth match
            case TlsClientAuth.None      => ()
            case TlsClientAuth.Requested => secure.setWantClientAuth(true)
            case TlsClientAuth.Required  => secure.setNeedClientAuth(true)
          secure
        catch
          case error: Throwable =>
            socket.close()
            throw error

  private def brokerFields: Map[String, String] = Map(
    "node_id" -> config.nodeId.toString,
    "data_directory" -> config.dataDirectory.toAbsolutePath.normalize().toString
  )

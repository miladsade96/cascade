package cascade.qualification

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.cluster.ClusterNode
import cascade.security.*
import cascade.storage.FlushPolicy
import java.net.ServerSocket
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.nio.file.{AccessDeniedException, AtomicMoveNotSupportedException, Files, Path, StandardCopyOption}
import java.time.{Duration, Instant}
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{CloseOptions, ConsumerConfig, ConsumerRebalanceListener, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Authenticated RF=3 workload used by the externally scheduled multi-day release gate. */
object AuthenticatedClusterSoak:
  def run(config: SoakConfig): SoakReport =
    require(config.profile == SoakProfile.AuthenticatedCluster, "authenticated runner requires the authenticated-cluster profile")
    val root = config.dataDirectory.getOrElse(Files.createTempDirectory("cascade-authenticated-soak"))
    Files.createDirectories(root): Unit
    val startedAt = Instant.now()
    val started = System.nanoTime()
    val revision = SoakEvidence.currentRevision()
    val metrics = SoakMetricAccumulator()
    var counters = SoakCounters()
    var cluster: SecureSoakCluster = null
    var clients = Vector.empty[TenantSoakClient]
    var failure: Option[String] = None
    try
      cluster = SecureSoakCluster(root, config)
      cluster.startAll()
      cluster.awaitReady(Duration.ofSeconds(45))
      cluster.awaitDistributedQuotas(Duration.ofSeconds(30))
      createTopics(cluster, config)
      clients = Vector.tabulate(config.tenants) { tenant => TenantSoakClient(cluster, config, tenant) }
      val workloadStarted = System.nanoTime()
      val deadline = workloadStarted + Duration.ofSeconds(config.durationSeconds).toNanos
      var nextReport = workloadStarted + Duration.ofSeconds(config.reportIntervalSeconds).toNanos
      var nextRebalance = workloadStarted + Duration.ofSeconds(config.rebalanceIntervalSeconds).toNanos
      var nextChurn = workloadStarted + Duration.ofSeconds(config.churnIntervalSeconds).toNanos
      var tlsRotated = false
      while System.nanoTime() < deadline || counters.cycles == 0L do
        val cycle = counters.cycles + 1L
        clients.foreach { client =>
          val result = client.runCycle(cycle)
          counters = counters.copy(
            records = counters.records + result.committed,
            verifiedRecords = counters.verifiedRecords + result.verified,
            abortedRecords = counters.abortedRecords + result.aborted,
            mismatches = counters.mismatches + result.mismatches
          )
        }
        counters = counters.copy(cycles = cycle, peakHeapBytes = math.max(counters.peakHeapBytes, usedHeapBytes()))
        metrics.sample(cluster)

        val now = System.nanoTime()
        if !tlsRotated && config.tlsRotationAfterSeconds > 0L &&
            now - workloadStarted >= Duration.ofSeconds(config.tlsRotationAfterSeconds).toNanos then
          cluster.rotateTls()
          tlsRotated = true
          counters = counters.copy(tlsRotations = counters.tlsRotations + 1L)

        if config.rebalanceIntervalSeconds > 0L && now >= nextRebalance then
          verifyRebalance(cluster, tenant = (counters.rebalances % config.tenants).toInt, cycle)
          counters = counters.copy(rebalances = counters.rebalances + 1L)
          nextRebalance = System.nanoTime() + Duration.ofSeconds(config.rebalanceIntervalSeconds).toNanos

        if config.churnIntervalSeconds > 0L && now >= nextChurn then
          metrics.sample(cluster)
          val failedOver = cluster.churnController()
          counters = counters.copy(
            brokerRestarts = counters.brokerRestarts + 1L,
            controllerFailovers = counters.controllerFailovers + (if failedOver then 1L else 0L)
          )
          nextChurn = System.nanoTime() + Duration.ofSeconds(config.churnIntervalSeconds).toNanos

        if System.nanoTime() >= nextReport then
          val elapsed = (System.nanoTime() - workloadStarted) / 1_000_000_000d
          println(
            f"SOAK_PROGRESS profile=${config.profile.cliName} elapsed_seconds=$elapsed%.1f records=${counters.records} " +
              s"verified=${counters.verifiedRecords} cycles=${counters.cycles} mismatches=${counters.mismatches} " +
              s"rebalances=${counters.rebalances} restarts=${counters.brokerRestarts} tls_rotations=${counters.tlsRotations}"
          )
          writeProgress(config, startedAt, revision, started, counters, metrics, None)
          nextReport = System.nanoTime() + Duration.ofSeconds(config.reportIntervalSeconds).toNanos

      val expected = clients.map(_.expectedRecords)
      clients.foreach(_.close())
      clients = Vector.empty
      if config.finalRestart then
        metrics.sample(cluster)
        cluster.restartAll()
        val recovered = expected.zipWithIndex.map { case (records, tenant) =>
          verifyRecovered(cluster, config, tenant, records)
        }.sum
        counters = counters.copy(recoveredRecords = recovered, restartVerified = recovered == counters.records)
        if recovered != counters.records then counters = counters.copy(mismatches = counters.mismatches + 1L)

      metrics.sample(cluster)
      val snapshot = metrics.snapshot
      counters = counters.copy(
        quotaReservations = snapshot.quotaReservations,
        quotaThrottled = snapshot.quotaThrottled,
        quotaRejected = snapshot.quotaRejected,
        authenticationSuccesses = snapshot.authenticationSuccesses,
        authenticationFailures = snapshot.authenticationFailures,
        authenticatedTenants = cluster.authenticatedTenantCount,
        peakHeapBytes = math.max(counters.peakHeapBytes, usedHeapBytes())
      )
      validateRequiredEvidence(config, counters)
    catch
      case NonFatal(error) =>
        failure = Some(s"${error.getClass.getSimpleName}: ${Option(error.getMessage).getOrElse("no message")}")
        counters = counters.copy(mismatches = counters.mismatches + 1L)
    finally
      clients.foreach(client => try client.close() catch case NonFatal(_) => ())
      if cluster != null then
        try
          metrics.sample(cluster)
          cluster.close()
        catch case NonFatal(error) => if failure.isEmpty then failure = Some(s"cleanup failed: ${error.getMessage}")

    val snapshot = metrics.snapshot
    counters = counters.copy(
      quotaReservations = math.max(counters.quotaReservations, snapshot.quotaReservations),
      quotaThrottled = math.max(counters.quotaThrottled, snapshot.quotaThrottled),
      quotaRejected = math.max(counters.quotaRejected, snapshot.quotaRejected),
      authenticationSuccesses = math.max(counters.authenticationSuccesses, snapshot.authenticationSuccesses),
      authenticationFailures = math.max(counters.authenticationFailures, snapshot.authenticationFailures)
    )
    val report = SoakReport(
      SoakProfile.AuthenticatedCluster,
      startedAt,
      Instant.now(),
      config.durationSeconds,
      (System.nanoTime() - started) / 1_000_000_000d,
      config.tenants,
      revision,
      counters,
      failure
    )
    config.reportPath.foreach(SoakEvidence.writeAtomically(_, report))
    if config.keepData || config.dataDirectory.nonEmpty || failure.nonEmpty then println(s"SOAK_DATA_DIRECTORY ${root.toAbsolutePath}")
    else SecurityTestSupport.deleteTree(root)
    report

  private def createTopics(cluster: SecureSoakCluster, config: SoakConfig): Unit =
    val admin = Admin.create(cluster.clientProperties(0))
    try
      val topics = Vector.tabulate(config.tenants)(tenant => NewTopic(topicName(tenant), 1, 3.toShort))
      admin.createTopics(topics.asJava).all().get(45L, TimeUnit.SECONDS): Unit
    finally admin.close(Duration.ofSeconds(5))

  private def verifyRebalance(cluster: SecureSoakCluster, tenant: Int, cycle: Long): Unit =
    val firstTracker = RebalanceTracker()
    val secondTracker = RebalanceTracker()
    val first = KafkaConsumer[Array[Byte], Array[Byte]](rebalanceProperties(cluster, tenant, cycle, "first"))
    val second = KafkaConsumer[Array[Byte], Array[Byte]](rebalanceProperties(cluster, tenant, cycle, "second"))
    try
      first.subscribe(java.util.List.of(topicName(tenant)), firstTracker)
      pollUntil(Duration.ofSeconds(15)) { first.poll(Duration.ofMillis(100)); firstTracker.assignments > 0L }
      second.subscribe(java.util.List.of(topicName(tenant)), secondTracker)
      pollUntil(Duration.ofSeconds(20)) {
        first.poll(Duration.ofMillis(100))
        second.poll(Duration.ofMillis(100))
        firstTracker.revocations > 0L && secondTracker.assignments > 0L
      }
    finally
      second.close(CloseOptions.timeout(Duration.ofSeconds(3)))
      first.close(CloseOptions.timeout(Duration.ofSeconds(3)))

  private def verifyRecovered(
      cluster: SecureSoakCluster,
      config: SoakConfig,
      tenant: Int,
      expected: Vector[ExpectedSoakRecord]
  ): Long =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](verifierProperties(cluster, tenant, s"recovery-$tenant"))
    val partition = TopicPartition(topicName(tenant), 0)
    try
      consumer.assign(java.util.List.of(partition))
      consumer.seekToBeginning(java.util.List.of(partition))
      var verified = 0
      val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos
      while verified < expected.size && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(200)).iterator().asScala.foreach { record =>
          if verified >= expected.size then throw IllegalStateException(s"tenant $tenant recovered an unexpected extra record")
          val wanted = expected(verified)
          if record.offset() != wanted.offset || !java.util.Arrays.equals(record.value(), wanted.value) then
            throw IllegalStateException(
              s"tenant $tenant recovery mismatch at index $verified: offset=${record.offset()} expected=${wanted.offset}"
            )
          verified += 1
        }
      if verified != expected.size then throw IllegalStateException(s"tenant $tenant recovered $verified/${expected.size} records")
      verified.toLong
    finally consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)))

  private def validateRequiredEvidence(config: SoakConfig, counters: SoakCounters): Unit =
    require(counters.records > 0L, "authenticated soak produced no committed records")
    require(counters.verifiedRecords == counters.records, "authenticated soak did not verify every committed record")
    require(counters.authenticationSuccesses > 0L, "authenticated soak recorded no successful authentication")
    require(counters.authenticationFailures == 0L, "authenticated soak recorded authentication failures")
    require(counters.authenticatedTenants == config.tenants, "authenticated soak did not audit every tenant principal")
    require(counters.quotaReservations > 0L, "authenticated soak recorded no distributed quota reservations")
    require(counters.quotaRejected == 0L, "authenticated soak exceeded its bounded quota throttle")
    if config.finalRestart then require(counters.restartVerified, "authenticated soak did not verify the final restart")
    if config.tlsRotationAfterSeconds > 0L && config.durationSeconds >= config.tlsRotationAfterSeconds then
      require(counters.tlsRotations > 0L, "authenticated soak did not execute TLS rotation")
    if config.rebalanceIntervalSeconds > 0L && config.durationSeconds >= config.rebalanceIntervalSeconds then
      require(counters.rebalances > 0L, "authenticated soak did not execute a consumer rebalance")
    if config.churnIntervalSeconds > 0L && config.durationSeconds >= config.churnIntervalSeconds then
      require(counters.controllerFailovers > 0L, "authenticated soak did not execute controller failover")

  private def writeProgress(
      config: SoakConfig,
      startedAt: Instant,
      revision: String,
      startedNanos: Long,
      counters: SoakCounters,
      metrics: SoakMetricAccumulator,
      failure: Option[String]
  ): Unit =
    config.reportPath.foreach { path =>
      val snapshot = metrics.snapshot
      SoakEvidence.writeAtomically(
        path,
        SoakReport(
          config.profile,
          startedAt,
          Instant.now(),
          config.durationSeconds,
          (System.nanoTime() - startedNanos) / 1_000_000_000d,
          config.tenants,
          revision,
          counters.copy(
            quotaReservations = snapshot.quotaReservations,
            quotaThrottled = snapshot.quotaThrottled,
            quotaRejected = snapshot.quotaRejected,
            authenticationSuccesses = snapshot.authenticationSuccesses,
            authenticationFailures = snapshot.authenticationFailures
          ),
          failure.orElse(Some("campaign still running"))
        )
      )
    }

  private def rebalanceProperties(cluster: SecureSoakCluster, tenant: Int, cycle: Long, member: String): Properties =
    val properties = cluster.clientProperties(tenant)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, s"soak-rebalance-$tenant")
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, s"soak-rebalance-$tenant-$cycle-$member")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000")
    properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    properties.put("group.protocol", "classic")
    properties

  private[qualification] def verifierProperties(cluster: SecureSoakCluster, tenant: Int, clientId: String): Properties =
    val properties = cluster.clientProperties(tenant)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    properties

  private def pollUntil(timeout: Duration)(operation: => Boolean): Unit =
    val deadline = System.nanoTime() + timeout.toNanos
    var complete = false
    while !complete && System.nanoTime() < deadline do complete = operation
    if !complete then throw IllegalStateException(s"operation did not complete within $timeout")

  private def usedHeapBytes(): Long = java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed

  private[qualification] def topicName(tenant: Int): String = s"soak-tenant-$tenant"

private final case class CycleResult(committed: Long, verified: Long, aborted: Long, mismatches: Long)
private final case class ExpectedSoakRecord(offset: Long, value: Array[Byte])

private final class TenantSoakClient(cluster: SecureSoakCluster, config: SoakConfig, tenant: Int) extends AutoCloseable:
  private val producerProperties = cluster.clientProperties(tenant)
  producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
  producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
  producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, s"soak-transaction-$tenant")
  producerProperties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000")
  producerProperties.put(ProducerConfig.ACKS_CONFIG, "all")
  producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
  producerProperties.put(ProducerConfig.CLIENT_ID_CONFIG, s"soak-producer-$tenant")
  private val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
  private val consumer = KafkaConsumer[Array[Byte], Array[Byte]](
    AuthenticatedClusterSoak.verifierProperties(cluster, tenant, s"soak-verifier-$tenant")
  )
  private val partition = TopicPartition(AuthenticatedClusterSoak.topicName(tenant), 0)
  private val expected = mutable.ArrayBuffer.empty[ExpectedSoakRecord]
  private var verified = 0
  private var sequence = 0L
  producer.initTransactions()
  consumer.assign(java.util.List.of(partition))
  consumer.seekToBeginning(java.util.List.of(partition))

  def expectedRecords: Vector[ExpectedSoakRecord] = expected.toVector

  def runCycle(cycle: Long): CycleResult =
    var aborted = 0L
    if config.abortEveryCycles > 0 && cycle % config.abortEveryCycles == 0L then
      producer.beginTransaction()
      producer.send(
        ProducerRecord(
          AuthenticatedClusterSoak.topicName(tenant),
          0,
          longBytes(-cycle),
          payload(tenant, -cycle, config.payloadBytes)
        )
      ).get(30L, TimeUnit.SECONDS)
      producer.abortTransaction()
      aborted = 1L

    producer.beginTransaction()
    val pending = Vector.tabulate(config.recordsPerCycle) { index =>
      val current = sequence + index.toLong
      val value = payload(tenant, current, config.payloadBytes)
      val metadata = producer.send(
        ProducerRecord(AuthenticatedClusterSoak.topicName(tenant), 0, longBytes(current), value)
      ).get(30L, TimeUnit.SECONDS)
      ExpectedSoakRecord(metadata.offset(), value)
    }
    producer.commitTransaction()
    expected ++= pending
    sequence += pending.size
    val before = verified
    var mismatches = 0L
    val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
    while verified < expected.size && System.nanoTime() < deadline do
      consumer.poll(Duration.ofMillis(100)).iterator().asScala.foreach { record =>
        if verified >= expected.size then mismatches += 1L
        else
          val wanted = expected(verified)
          if record.offset() != wanted.offset || !java.util.Arrays.equals(record.value(), wanted.value) then mismatches += 1L
          verified += 1
      }
    if verified != expected.size then throw IllegalStateException(s"tenant $tenant stalled at $verified/${expected.size} records")
    CycleResult(pending.size.toLong, (verified - before).toLong, aborted, mismatches)

  override def close(): Unit =
    try consumer.close(CloseOptions.timeout(Duration.ofSeconds(5)))
    finally producer.close(Duration.ofSeconds(5))

  private def longBytes(value: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(value).array()

  private def payload(tenant: Int, sequence: Long, bytes: Int): Array[Byte] =
    val value = new Array[Byte](bytes)
    ByteBuffer.wrap(value).putInt(tenant).putLong(sequence)
    var index = 12
    while index < value.length do
      value(index) = ((tenant * 31L + sequence * 17L + index) & 0xff).toByte
      index += 1
    value

private final class RebalanceTracker extends ConsumerRebalanceListener:
  @volatile var assignments = 0L
  @volatile var revocations = 0L

  override def onPartitionsAssigned(partitions: java.util.Collection[TopicPartition]): Unit = assignments += 1L
  override def onPartitionsRevoked(partitions: java.util.Collection[TopicPartition]): Unit = revocations += 1L

private final case class MetricTotals(
    quotaReservations: Long,
    quotaThrottled: Long,
    quotaRejected: Long,
    authenticationSuccesses: Long,
    authenticationFailures: Long
)

private final class SoakMetricAccumulator:
  private val archived = mutable.HashMap.empty[Int, MetricTotals].withDefaultValue(MetricTotals(0L, 0L, 0L, 0L, 0L))
  private val previous = mutable.HashMap.empty[Int, MetricTotals]

  def sample(cluster: SecureSoakCluster): Unit =
    cluster.runningBrokers.foreach { broker =>
      val metrics = broker.metricsSnapshot
      val authenticationSuccesses = metrics.authentication.mechanisms.map(_.successes).sum
      val authenticationFailures = metrics.authentication.mechanisms.map(_.failures).sum
      val current = MetricTotals(
        metrics.distributedQuota.reservations,
        metrics.distributedQuota.throttled,
        metrics.distributedQuota.rejected,
        authenticationSuccesses,
        authenticationFailures
      )
      previous.get(metrics.nodeId).foreach { prior =>
        if current.quotaReservations < prior.quotaReservations || current.authenticationSuccesses < prior.authenticationSuccesses then
          archived.update(metrics.nodeId, add(archived(metrics.nodeId), prior))
      }
      previous.update(metrics.nodeId, current)
    }

  def snapshot: MetricTotals =
    (archived.values ++ previous.values).foldLeft(MetricTotals(0L, 0L, 0L, 0L, 0L))(add)

  private def add(first: MetricTotals, second: MetricTotals): MetricTotals =
    MetricTotals(
      first.quotaReservations + second.quotaReservations,
      first.quotaThrottled + second.quotaThrottled,
      first.quotaRejected + second.quotaRejected,
      first.authenticationSuccesses + second.authenticationSuccesses,
      first.authenticationFailures + second.authenticationFailures
    )

private final class SecureSoakCluster private (root: Path, config: SoakConfig) extends AutoCloseable:
  private val nodeIds = Vector(1, 2, 3)
  private val ports = freePorts(nodeIds.size)
  val nodes: Vector[ClusterNode] = nodeIds.zip(ports).map { case (nodeId, port) => ClusterNode(nodeId, "localhost", port) }
  private val certificateValidityDays = math.max(7L, (config.durationSeconds + 86399L) / 86400L + 2L).toInt
  private val oldMaterial = SecurityTestSupport.createMutualTlsMaterial(
    Files.createDirectories(root.resolve("tls-old")), nodeIds, validityDays = certificateValidityDays
  )
  private val newMaterial = SecurityTestSupport.createMutualTlsMaterial(
    Files.createDirectories(root.resolve("tls-new")), nodeIds, validityDays = certificateValidityDays
  )
  val clientTrustStore: Path = SecurityTestSupport.combineTrustStores(
    root, "client-trust.p12", Vector(oldMaterial.trustStore, newMaterial.trustStore)
  )
  private val activeTrustStore = root.resolve("active-trust.p12")
  replaceAtomically(oldMaterial.trustStore, activeTrustStore)
  private val activeKeyStores = nodeIds.map { nodeId =>
    val target = root.resolve(s"active-broker-$nodeId.p12")
    replaceAtomically(oldMaterial.keyStores(nodeId), target)
    nodeId -> target
  }.toMap
  private val identities = root.resolve("peer-identities.conf")
  Files.writeString(
    identities,
    oldMaterial.principals.toVector.sortBy(_._1).map { case (nodeId, principal) => s"$nodeId $principal" }.mkString("", "\n", "\n"),
    StandardCharsets.UTF_8
  ): Unit
  private val credentials = root.resolve("soak-users.conf")
  Files.writeString(
    credentials,
    Vector.tabulate(config.tenants) { tenant =>
      val password = passwordFor(tenant).toCharArray
      try s"tenant-$tenant=${CredentialHash.create(password, CredentialHash.MinimumIterations)}"
      finally java.util.Arrays.fill(password, '\u0000')
    }.mkString("", "\n", "\n"),
    StandardCharsets.UTF_8
  ): Unit
  private val acls = root.resolve("soak-acls.conf")
  Files.writeString(
    acls,
    Vector.tabulate(config.tenants) { tenant =>
      Vector(
        s"allow tenant-$tenant All Topic ${AuthenticatedClusterSoak.topicName(tenant)}",
        s"allow tenant-$tenant All Group soak-rebalance-$tenant",
        s"allow tenant-$tenant All TransactionalId soak-transaction-$tenant"
      )
    }.flatten.mkString("", "\n", "\n"),
    StandardCharsets.UTF_8
  ): Unit
  private val quotaRate = config.quotaBytesPerSecond
  private val resources = ResourceLimits(
    requestBytesPerSecond = quotaRate,
    requestBurstBytes = quotaRate,
    responseBytesPerSecond = quotaRate,
    responseBurstBytes = quotaRate,
    produceBytesPerSecond = quotaRate,
    produceBurstBytes = quotaRate,
    fetchBytesPerSecond = quotaRate,
    fetchBurstBytes = quotaRate,
    maxThrottleMillis = 30000L
  )
  private val configs = nodes.map { node =>
    val tls = TlsConfig(
      keyStore = Some(activeKeyStores(node.id)),
      keyStorePassword = Some(SecurityTestSupport.StorePassword),
      trustStore = Some(activeTrustStore),
      trustStorePassword = Some(SecurityTestSupport.StorePassword),
      clientAuth = TlsClientAuth.Requested,
      reloadIntervalMillis = 100L
    )
    node.id -> BrokerConfig(
      bindHost = "127.0.0.1",
      port = node.port,
      advertisedHost = node.host,
      advertisedPort = Some(node.port),
      dataDirectory = Files.createDirectories(root.resolve(s"data-${node.id}")),
      flushPolicy = FlushPolicy.Sync,
      nodeId = node.id,
      clusterNodes = nodes,
      controllerId = 1,
      defaultReplicationFactor = 3,
      minInSyncReplicas = 2,
      peerTimeoutMillis = 3000,
      controllerHeartbeatMillis = 100,
      controllerElectionTimeoutMillis = 600,
      autoCreateTopics = false,
      security = BrokerSecurityConfig(
        protocol = SecurityProtocol.SaslSsl,
        tls = tls,
        authentication = AuthenticationConfig(
          credentialsFile = Some(credentials),
          mechanisms = Vector(SaslMechanism.Plain),
          reloadIntervalMillis = 100L
        ),
        authorization = AuthorizationConfig(
          aclFile = Some(acls),
          reloadIntervalMillis = 100L,
          superUsers = Set("tenant-0") ++ oldMaterial.principals.values
        ),
        audit = AuditConfig(Some(root.resolve(s"audit-${node.id}.jsonl"))),
        resources = resources,
        peer = PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(identities), 100L)
      )
    )
  }.toMap
  private val running = mutable.HashMap.empty[Int, KafkaBroker]

  def bootstrapServers: String = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
  def runningBrokers: Vector[KafkaBroker] = running.values.toVector

  def clientProperties(tenant: Int): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "15000")
    properties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "45000")
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    properties.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"tenant-$tenant\" password=\"${passwordFor(tenant)}\";"
    )
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, clientTrustStore.toString)
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
    properties.put("enable.metrics.push", "false")
    properties

  def startAll(): Unit = nodeIds.foreach(start)

  def start(nodeId: Int): Unit =
    require(!running.contains(nodeId), s"broker $nodeId is already running")
    val broker = KafkaBroker(configs(nodeId))
    broker.start()
    running.update(nodeId, broker)

  def stop(nodeId: Int): Unit = running.remove(nodeId).foreach(_.close())

  def awaitReady(timeout: Duration): Unit =
    eventually(timeout) {
      running.size == nodeIds.size && running.values.forall(broker => broker.healthSnapshot.ready && !broker.metricsSnapshot.brokerFenced)
    }

  def awaitDistributedQuotas(timeout: Duration): Unit =
    eventually(timeout) {
      running.valuesIterator.exists { broker =>
        try broker.reserveClusterQuota(QuotaKind.Request, "User:soak-activation", 1).nonEmpty
        catch case NonFatal(_) => false
      }
    }

  def rotateTls(): Unit =
    val beforeTrust = running.view.mapValues(_.metricsSnapshot.tlsReload.generation).toMap
    replaceAtomically(clientTrustStore, activeTrustStore)
    eventually(Duration.ofSeconds(20)) {
      running.forall { case (nodeId, broker) => broker.metricsSnapshot.tlsReload.generation > beforeTrust(nodeId) }
    }
    nodeIds.foreach { nodeId =>
      val before = running(nodeId).metricsSnapshot.tlsReload.generation
      replaceAtomically(newMaterial.keyStores(nodeId), activeKeyStores(nodeId))
      eventually(Duration.ofSeconds(20)) { running(nodeId).metricsSnapshot.tlsReload.generation > before }
    }
    val beforeFinalTrust = running.view.mapValues(_.metricsSnapshot.tlsReload.generation).toMap
    replaceAtomically(newMaterial.trustStore, activeTrustStore)
    eventually(Duration.ofSeconds(20)) {
      running.forall { case (nodeId, broker) => broker.metricsSnapshot.tlsReload.generation > beforeFinalTrust(nodeId) }
    }
    require(running.values.forall(_.healthSnapshot.ready), "TLS rotation left a broker unready")

  def churnController(): Boolean =
    val previous = currentController(Duration.ofSeconds(15))
    stop(previous)
    val replacement = currentController(Duration.ofSeconds(20), excluded = Set(previous))
    start(previous)
    awaitReady(Duration.ofSeconds(30))
    replacement != previous

  def restartAll(): Unit =
    nodeIds.foreach(stop)
    nodeIds.foreach(start)
    awaitReady(Duration.ofSeconds(45))

  def authenticatedTenantCount: Int =
    val audit = nodeIds.iterator.map(nodeId => root.resolve(s"audit-$nodeId.jsonl"))
      .filter(Files.exists(_)).map(Files.readString(_)).mkString("\n")
    (0 until config.tenants).count(tenant => audit.contains(s"\"principal\":\"tenant-$tenant\""))

  private def currentController(timeout: Duration, excluded: Set[Int] = Set.empty): Int =
    var result = -1
    eventually(timeout) {
      result = running.valuesIterator.map(_.metricsSnapshot.controllerId)
        .find(id => !excluded(id) && running.contains(id)).getOrElse(-1)
      result > 0 && running(result).healthSnapshot.ready
    }
    result

  override def close(): Unit = nodeIds.foreach(stop)

  private def eventually(timeout: Duration)(condition: => Boolean): Unit =
    val deadline = System.nanoTime() + timeout.toNanos
    var complete = false
    while !complete && System.nanoTime() < deadline do
      complete = try condition catch case NonFatal(_) => false
      if !complete then Thread.sleep(50L)
    if !complete then throw IllegalStateException(s"cluster condition did not complete within $timeout")

  private def freePorts(count: Int): Vector[Int] =
    val sockets = Vector.fill(count)(ServerSocket(0))
    try sockets.map(_.getLocalPort)
    finally sockets.foreach(_.close())

  private def replaceAtomically(source: Path, target: Path): Unit =
    val staged = Files.createTempFile(target.getParent, s".${target.getFileName.toString}.", ".replacement")
    try
      Files.copy(source, staged, StandardCopyOption.REPLACE_EXISTING): Unit
      var moved = false
      var attempts = 0
      while !moved && attempts < 100 do
        attempts += 1
        try
          try Files.move(staged, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING): Unit
          catch case _: AtomicMoveNotSupportedException => Files.move(staged, target, StandardCopyOption.REPLACE_EXISTING): Unit
          moved = true
        catch case _: AccessDeniedException if attempts < 100 => Thread.sleep(10L)
      if !moved then throw AccessDeniedException(target.toString)
    finally Files.deleteIfExists(staged): Unit

  private def passwordFor(tenant: Int): String = s"cascade-soak-tenant-$tenant-password"

private object SecureSoakCluster:
  def apply(root: Path, config: SoakConfig): SecureSoakCluster = new SecureSoakCluster(root, config)

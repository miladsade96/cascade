package cascade.qualification

import cascade.cluster.{MetadataStore, PeerCapabilities}
import cascade.fault.BrokerProcess
import java.net.{ServerSocket, URI}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

final case class RollingUpgradeConfig(
    oldRuntime: Path,
    currentRuntime: Path,
    oldVersion: String,
    currentVersion: String,
    oldRevision: String,
    currentRevision: String,
    reportPath: Path,
    keepData: Boolean = false,
    oldFeatures: Map[String, Short] = Map.empty
)

object RollingUpgradeConfig:
  def parse(arguments: Array[String]): RollingUpgradeConfig =
    @annotation.tailrec
    def loop(values: List[String], options: Map[String, String], keepData: Boolean): (Map[String, String], Boolean) =
      values match
        case Nil => options -> keepData
        case "--keep-data" :: tail => loop(tail, options, true)
        case option :: value :: tail if option.startsWith("--") => loop(tail, options.updated(option.drop(2), value), keepData)
        case option :: _ => throw IllegalArgumentException(s"unknown or incomplete rolling-upgrade option: $option")

    val (options, keepData) = loop(arguments.toList, Map.empty, false)
    val allowed = Set("old-runtime", "current-runtime", "old-version", "current-version", "old-revision", "current-revision", "report", "old-features")
    require(options.keySet.subsetOf(allowed), "unknown rolling-upgrade option")
    def required(name: String): String = options.getOrElse(name, throw IllegalArgumentException(s"missing --$name"))
    RollingUpgradeConfig(
      Paths.get(required("old-runtime")).toAbsolutePath.normalize(),
      Paths.get(required("current-runtime")).toAbsolutePath.normalize(),
      required("old-version"),
      required("current-version"),
      required("old-revision"),
      required("current-revision"),
      Paths.get(required("report")).toAbsolutePath.normalize(),
      keepData,
      parseFeatures(options.getOrElse("old-features", ""))
    )

  def parseFeatures(value: String): Map[String, Short] =
    if value.isEmpty then Map.empty
    else
      val entries = value.split(",", -1).toVector.map { item =>
        val pair = item.split(":", -1)
        require(pair.length == 2 && pair(0).matches("[a-z][a-z0-9-]*"), "invalid baseline feature")
        val level = pair(1).toShort
        require(level > 0, "baseline feature levels must be positive")
        pair(0) -> level
      }
      require(entries.map(_._1).distinct.size == entries.size, "duplicate baseline feature")
      entries.toMap

final case class RollingUpgradeReport(
    status: String,
    startedAt: Instant,
    elapsedSeconds: Double,
    oldVersion: String,
    oldRevision: String,
    oldArtifactSha256: String,
    currentVersion: String,
    currentRevision: String,
    currentArtifactSha256: String,
    records: Int,
    rollbackSucceeded: Boolean,
    featuresActivated: Boolean,
    downgradeRejected: Boolean,
    phases: Vector[String],
    error: Option[String] = None
):
  def json: String =
    val phaseJson = phases.map(value => s"\"${escape(value)}\"").mkString("[", ",", "]")
    val errorJson = error.map(value => s"\"${escape(value)}\"").getOrElse("null")
    f"""{"status":"${escape(status)}","started_at":"$startedAt","elapsed_seconds":$elapsedSeconds%.3f,"old_version":"${escape(oldVersion)}","old_revision":"${escape(oldRevision)}","old_artifact_sha256":"$oldArtifactSha256","current_version":"${escape(currentVersion)}","current_revision":"${escape(currentRevision)}","current_artifact_sha256":"$currentArtifactSha256","records":$records,"rollback_succeeded":$rollbackSucceeded,"features_activated":$featuresActivated,"downgrade_rejected":$downgradeRejected,"phases":$phaseJson,"error":$errorJson}"""

  private def escape(value: String): String =
    value.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case character => character.toString
    }

object RollingUpgradeQualification:
  private val TrafficTopic = "rolling-upgrade-events"
  private val ActivationTopic = "rolling-feature-activation"
  private val RecordsPerPhase = 5

  def main(arguments: Array[String]): Unit =
    val config = RollingUpgradeConfig.parse(arguments)
    val startedAt = Instant.now()
    val startedNanos = System.nanoTime()
    val phases = ArrayBuffer.empty[String]
    val oldArtifact = applicationJar(config.oldRuntime, config.oldVersion)
    val currentArtifact = applicationJar(config.currentRuntime, config.currentVersion)
    var records = 0
    var rollbackSucceeded = false
    var featuresActivated = false
    var downgradeRejected = false
    try
      records = run(
        config,
        phases,
        value => records = value,
        () => rollbackSucceeded = true,
        () => featuresActivated = true,
        () => downgradeRejected = true
      )
      val report = RollingUpgradeReport(
        "passed",
        startedAt,
        elapsedSeconds(startedNanos),
        config.oldVersion,
        config.oldRevision,
        sha256(oldArtifact),
        config.currentVersion,
        config.currentRevision,
        sha256(currentArtifact),
        records,
        rollbackSucceeded,
        featuresActivated,
        downgradeRejected,
        phases.toVector
      )
      writeReport(config.reportPath, report)
      println(s"ROLLING_UPGRADE_RESULT ${report.json}")
    catch
      case error: Throwable =>
        val report = RollingUpgradeReport(
          "failed",
          startedAt,
          elapsedSeconds(startedNanos),
          config.oldVersion,
          config.oldRevision,
          sha256(oldArtifact),
          config.currentVersion,
          config.currentRevision,
          sha256(currentArtifact),
          records,
          rollbackSucceeded,
          featuresActivated,
          downgradeRejected,
          phases.toVector,
          Some(Option(error.getMessage).getOrElse(error.getClass.getName))
        )
        writeReport(config.reportPath, report)
        println(s"ROLLING_UPGRADE_RESULT ${report.json}")
        throw error

  private def run(
      config: RollingUpgradeConfig,
      phases: ArrayBuffer[String],
      recordProgress: Int => Unit,
      markRollback: () => Unit,
      markActivated: () => Unit,
      markDowngradeRejected: () => Unit
  ): Int =
    val root = Files.createTempDirectory("cascade-rolling-upgrade")
    val nodes = freePorts(6)
    val brokerPorts = nodes.take(3)
    val operationsPorts = nodes.drop(3)
    val dataDirectories = Vector.tabulate(3)(index => Files.createDirectories(root.resolve(s"node-${index + 1}")))
    val clusterNodes = brokerPorts.zipWithIndex.map { case (port, index) => s"${index + 1}@127.0.0.1:$port" }.mkString(",")
    val processes = Array.fill[Option[BrokerProcess]](3)(None)
    val expected = ArrayBuffer.empty[String]
    val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()

    def arguments(nodeId: Int): Seq[String] = Seq(
      "--host", "127.0.0.1",
      "--port", brokerPorts(nodeId - 1).toString,
      "--advertised-host", "127.0.0.1",
      "--advertised-port", brokerPorts(nodeId - 1).toString,
      "--node-id", nodeId.toString,
      "--data-dir", dataDirectories(nodeId - 1).toString,
      "--cluster-nodes", clusterNodes,
      "--controller-id", "1",
      "--default-replication-factor", "3",
      "--min-insync-replicas", "2",
      "--peer-timeout-ms", "500",
      "--controller-heartbeat-ms", "100",
      "--controller-election-timeout-ms", "1000",
      "--flush-policy", "sync",
      "--operations-port", operationsPorts(nodeId - 1).toString
    )

    def startNode(nodeId: Int, runtime: Path): Unit =
      require(processes(nodeId - 1).isEmpty, s"node $nodeId is already running")
      val process = BrokerProcess.startRuntime(runtime, arguments(nodeId))
      processes(nodeId - 1) = Some(process)
      process.awaitListening("127.0.0.1", brokerPorts(nodeId - 1), 30000L)

    def stopNode(nodeId: Int): Unit =
      processes(nodeId - 1).foreach(_.stop())
      processes(nodeId - 1) = None

    def ready(port: Int): Boolean =
      val request = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port/ready"))
        .timeout(Duration.ofSeconds(2)).GET().build()
      try http.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200
      catch case _: Throwable => false

    def awaitReady(): Unit =
      val running = processes.zipWithIndex.collect { case (Some(_), index) => operationsPorts(index) }.toVector
      val deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos
      var healthy = false
      while !healthy && System.nanoTime() < deadline do
        healthy = running.forall(ready)
        if !healthy then Thread.sleep(100L)
      if !healthy then throw IllegalStateException(s"cluster did not become ready on operations ports ${running.mkString(",")}")

    def assertFeatures(active: Boolean): Unit =
      val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
      var observed = Vector.empty[Map[String, Short]]
      var matches = false
      while !matches && System.nanoTime() < deadline do
        observed = dataDirectories.map(readMetadata).map(_.featureLevels)
        matches = if active then observed.forall(_ == PeerCapabilities.Current.featureLevels) else observed.forall(_ == config.oldFeatures)
        if !matches then Thread.sleep(100L)
      if !matches then
        throw IllegalStateException(s"unexpected feature state: active=$active observed=$observed")

    def bootstrap: String = brokerPorts.map(port => s"127.0.0.1:$port").mkString(",")

    def produce(label: String): Unit =
      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap))
      try
        (0 until RecordsPerPhase).foreach { _ =>
          val offset = expected.size.toLong
          val value = s"$offset:$label"
          val metadata = producer.send(ProducerRecord(TrafficTopic, 0, null, value.getBytes(StandardCharsets.UTF_8)))
            .get(60, TimeUnit.SECONDS)
          if metadata.offset() != offset then
            throw IllegalStateException(s"unexpected offset during $label: expected $offset, got ${metadata.offset()}")
          expected += value
        }
      finally producer.close(Duration.ofSeconds(10))
      commitProgress(bootstrap, expected.size.toLong)
      phases += label
      recordProgress(expected.size)

    def restart(nodeId: Int, runtime: Path): Unit =
      stopNode(nodeId)
      startNode(nodeId, runtime)
      awaitReady()

    try
      (1 to 3).foreach(startNode(_, config.oldRuntime))
      awaitReady()
      produce(s"${config.oldVersion}-baseline")
      assertFeatures(active = false)

      restart(3, config.currentRuntime)
      assertFeatures(active = false)
      produce("upgrade-node-3")

      restart(3, config.oldRuntime)
      assertFeatures(active = false)
      produce("rollback-node-3-before-activation")
      markRollback()

      restart(3, config.currentRuntime)
      assertFeatures(active = false)
      produce("upgrade-node-3-again")

      restart(2, config.currentRuntime)
      assertFeatures(active = false)
      produce("upgrade-node-2")

      restart(1, config.currentRuntime)
      awaitReady()
      createTopic(bootstrap, ActivationTopic)
      assertFeatures(active = true)
      markActivated()
      produce("upgrade-node-1-and-activate")

      stopNode(3)
      val downgrade = BrokerProcess.startRuntime(config.oldRuntime, arguments(3))
      try
        if !downgrade.awaitExit(10L) then
          throw IllegalStateException(s"${config.oldVersion} did not fail closed after feature activation")
        val output = downgrade.output.mkString("\n")
        if !output.contains("unsupported cluster metadata format") then
          throw IllegalStateException(s"downgrade failed without the expected format rejection: $output")
      finally downgrade.close()
      phases += "reject-downgrade-after-activation"
      markDowngradeRejected()

      awaitReady()
      produce("traffic-after-rejected-downgrade")
      startNode(3, config.currentRuntime)
      awaitReady()
      produce(s"recover-node-3-on-${config.currentVersion}")
      consumeExact(bootstrap, expected.toVector)
      phases += "exact-consume-after-rolling-campaign"
      expected.size
    finally
      processes.flatten.foreach(_.stop())
      if config.keepData then println(s"ROLLING_UPGRADE_DATA_DIRECTORY ${root.toAbsolutePath}")
      else deleteTree(root)

  private def createTopic(bootstrap: String, topic: String): Unit =
    val admin = Admin.create(properties(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
      AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG -> "30000"
    ))
    try admin.createTopics(java.util.List.of(NewTopic(topic, 1, 3.toShort))).all().get(30, TimeUnit.SECONDS): Unit
    finally admin.close(Duration.ofSeconds(5))

  private def producerProperties(bootstrap: String): Properties = properties(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.ACKS_CONFIG -> "all",
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "false",
    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG -> "60000",
    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "5000",
    "enable.metrics.push" -> "false"
  )

  private def consumeExact(bootstrap: String, expected: Vector[String]): Unit =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](properties(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.GROUP_ID_CONFIG -> "rolling-progress",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      "enable.metrics.push" -> "false"
    ))
    try
      val partition = TopicPartition(TrafficTopic, 0)
      consumer.assign(java.util.List.of(partition))
      consumer.seekToBeginning(java.util.List.of(partition))
      val actual = ArrayBuffer.empty[String]
      val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
      while actual.size < expected.size && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(200)).iterator().asScala.foreach { record =>
          if actual.size < expected.size then
            if record.offset() != actual.size.toLong then
              throw IllegalStateException(s"non-contiguous consumed offset: expected ${actual.size}, got ${record.offset()}")
            actual += String(record.value(), StandardCharsets.UTF_8)
        }
      if actual.toVector != expected then
        throw IllegalStateException(s"exact consume mismatch: expected ${expected.size}, got ${actual.size}")
      val committed = consumer.committed(java.util.Set.of(partition)).get(partition)
      require(committed != null && committed.offset() == expected.size.toLong, "rolling consumer offset was not recovered exactly")
    finally consumer.close()

  private def commitProgress(bootstrap: String, offset: Long): Unit =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](properties(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
      ConsumerConfig.GROUP_ID_CONFIG -> "rolling-progress",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName
    ))
    try consumer.commitSync(Map(TopicPartition(TrafficTopic, 0) -> OffsetAndMetadata(offset)).asJava)
    finally consumer.close()

  private def readMetadata(dataDirectory: Path): cascade.cluster.ClusterMetadata =
    val source = dataDirectory.resolve(".cascade").resolve("cluster-metadata.log")
    val probeDirectory = Files.createTempDirectory("cascade-metadata-probe")
    val copy = probeDirectory.resolve("cluster-metadata.log")
    try
      Files.copy(source, copy, REPLACE_EXISTING): Unit
      val objects = cascade.cluster.ShardObjectStore.pathFor(source)
      if Files.exists(objects) then
        val targetObjects = cascade.cluster.ShardObjectStore.pathFor(copy)
        Files.createDirectories(targetObjects)
        val entries = Files.list(objects)
        try entries.iterator().asScala.filter(Files.isRegularFile(_)).foreach { entry =>
          Files.copy(entry, targetObjects.resolve(entry.getFileName), REPLACE_EXISTING): Unit
        }
        finally entries.close()
      val store = MetadataStore(copy)
      try store.metadata
      finally store.close()
    finally deleteTree(probeDirectory)

  private def applicationJar(runtime: Path, version: String): Path =
    val libraryDirectory = runtime.resolve("lib")
    require(Files.isDirectory(libraryDirectory), s"runtime library directory does not exist: $libraryDirectory")
    val files = Files.list(libraryDirectory)
    try
      files.iterator().asScala.find(_.getFileName.toString == s"cascade_3-$version.jar")
        .getOrElse(throw IllegalArgumentException(s"runtime $runtime does not contain cascade_3-$version.jar"))
    finally files.close()

  private def sha256(path: Path): String =
    val digest = MessageDigest.getInstance("SHA-256")
    val input = Files.newInputStream(path)
    try
      val buffer = new Array[Byte](64 * 1024)
      var read = input.read(buffer)
      while read >= 0 do
        if read > 0 then digest.update(buffer, 0, read)
        read = input.read(buffer)
    finally input.close()
    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString

  private def writeReport(path: Path, report: RollingUpgradeReport): Unit =
    Option(path.getParent).foreach(Files.createDirectories(_))
    val temporary = path.resolveSibling(path.getFileName.toString + ".tmp")
    Files.writeString(temporary, report.json + System.lineSeparator(), StandardCharsets.UTF_8): Unit
    try Files.move(temporary, path, ATOMIC_MOVE, REPLACE_EXISTING): Unit
    catch case _: java.nio.file.AtomicMoveNotSupportedException => Files.move(temporary, path, REPLACE_EXISTING): Unit

  private def properties(values: (String, String)*): Properties =
    val result = Properties()
    values.foreach { case (name, value) => result.put(name, value): Unit }
    result

  private def freePorts(count: Int): Vector[Int] =
    val sockets = Vector.fill(count)(ServerSocket(0))
    try sockets.map(_.getLocalPort)
    finally sockets.foreach(_.close())

  private def elapsedSeconds(startedNanos: Long): Double = (System.nanoTime() - startedNanos) / 1_000_000_000d

  private def deleteTree(root: Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

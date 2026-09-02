package cascade.qualification

import cascade.coordinator.CoordinatorProbe
import cascade.fault.FaultCluster
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.Properties
import java.util.concurrent.{Callable, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.jdk.CollectionConverters.*

final case class CoordinatorScaleReport(
    groups: Int, concurrency: Int, rounds: Int, verified: Int, writes: Int,
    seconds: Double, p50Millis: Double, p95Millis: Double, p99Millis: Double,
    checkpointAttempts: Long, checkpointFailures: Long, deltaBytes: Long, fullImageBytes: Long,
    owners: Vector[Int], controllerFailover: Boolean, restartRecovery: Boolean,
    revision: String, startedAt: Instant,
    journalDeltaBytes: Long, journalFullBytes: Long, journalCheckpointBytes: Long,
    replicationDeltaBytes: Long, replicationFullBytes: Long, replicationFallbacks: Long,
    clientLifecycle: String = "churn", clientsCreated: Int = 0, warmupSeconds: Double = 0d,
    objectWrittenBytes: Long = 0L, objectsWritten: Long = 0L, objectsReused: Long = 0L,
    objectReclaimedBytes: Long = 0L, objectStoredBytes: Long = 0L,
    maxConnectionsPerIp: Int = 1000, rejectedConnections: Long = 0L
):
  private def baseJson: String =
    f"""{"status":"passed","started_at":"$startedAt","revision":"$revision","release":"${cascade.BuildInfo.Version}","java_version":"${System.getProperty("java.version")}","available_processors":${Runtime.getRuntime.availableProcessors()},"groups":$groups,"concurrency":$concurrency,"rounds":$rounds,"verified":$verified,"writes":$writes,"write_seconds":$seconds%.3f,"writes_per_second":${writes / seconds}%.3f,"p50_ms":$p50Millis%.3f,"p95_ms":$p95Millis%.3f,"p99_ms":$p99Millis%.3f,"checkpoint_attempts":$checkpointAttempts,"checkpoint_failures":$checkpointFailures,"delta_bytes":$deltaBytes,"full_image_bytes":$fullImageBytes,"owner_ids":${owners.mkString("[", ",", "]")},"controller_failover":$controllerFailover,"restart_recovery":$restartRecovery,"journal_delta_bytes":$journalDeltaBytes,"journal_full_bytes":$journalFullBytes,"journal_checkpoint_bytes":$journalCheckpointBytes,"replication_delta_bytes":$replicationDeltaBytes,"replication_full_bytes":$replicationFullBytes,"replication_fallbacks":$replicationFallbacks}"""

  def json: String = baseJson.dropRight(1) +
    f""", "client_lifecycle":"$clientLifecycle","clients_created":$clientsCreated,"warmup_writes":${if clientLifecycle == "persistent" then groups else 0},"warmup_seconds":$warmupSeconds%.3f,"object_written_bytes":$objectWrittenBytes,"objects_written":$objectsWritten,"objects_reused":$objectsReused,"object_reclaimed_bytes":$objectReclaimedBytes,"object_stored_bytes":$objectStoredBytes,"max_connections_per_ip":$maxConnectionsPerIp,"rejected_connections":$rejectedConnections}"""

object CoordinatorScaleQualification:
  // Every client can retain bootstrap, metadata, and coordinator connections to one address.
  private[cascade] def connectionLimit(groups: Int, persistent: Boolean): Int =
    if persistent then math.max(1000, Math.addExact(Math.multiplyExact(groups, 3), 32)) else 1000

  def main(arguments: Array[String]): Unit =
    require(arguments.length % 2 == 0, "options require values")
    val pairs = arguments.grouped(2).map(a => a(0) -> a(1)).toVector
    require(pairs.map(_._1).distinct.size == pairs.size, "duplicate option")
    require(pairs.forall(p => Set("--groups", "--concurrency", "--rounds", "--report", "--client-lifecycle")(p._1)), "unknown option")
    val options = pairs.toMap
    val path = Path.of(options.getOrElse("--report", "artifacts/coordinator-scale.json")).toAbsolutePath
    Files.createDirectories(path.getParent)
    try
      val lifecycle = options.getOrElse("--client-lifecycle", "churn")
      require(Set("churn", "persistent")(lifecycle), "client lifecycle must be churn or persistent")
      val result = run(options.getOrElse("--groups", "1000").toInt, options.getOrElse("--concurrency", "8").toInt,
        options.getOrElse("--rounds", "2").toInt, persistent = lifecycle == "persistent")
      Files.writeString(path, result.json + "\n")
      println(s"COORDINATOR_SCALE_RESULT ${result.json}")
    catch
      case error: Exception =>
        Files.writeString(path, s"""{"status":"failed","at":"${Instant.now()}"}""" + "\n")
        throw error

  def run(groupCount: Int, concurrency: Int, rounds: Int, persistent: Boolean = false): CoordinatorScaleReport =
    require(groupCount >= 3 && groupCount <= 100000, "groups must be between 3 and 100000")
    require(concurrency > 0 && concurrency <= 64, "concurrency must be between 1 and 64")
    require(rounds > 0 && rounds <= 100, "rounds must be between 1 and 100")
    require(!persistent || groupCount <= 2000, "persistent mode is bounded to 2000 simultaneously resident clients")
    val startedAt = Instant.now()
    val revision = currentRevision()
    val perIpLimit = connectionLimit(groupCount, persistent)
    val cluster = FaultCluster(3, recordCalls = false, maxConnectionsPerIp = perIpLimit)
    val executor = Executors.newFixedThreadPool(concurrency)
    val partition = TopicPartition("coordinator-qualification", 0)
    val latencies = new java.util.concurrent.ConcurrentLinkedQueue[Long]()
    var writeNanos = 0L
    var warmupSeconds = 0d
    var phase = "startup"
    def enter(next: String): Unit =
      phase = next
      println(s"COORDINATOR_SCALE_PHASE $phase")
    val clientsCreated = AtomicInteger()
    val clients = new Array[KafkaConsumer[Array[Byte], Array[Byte]]](if persistent then groupCount else 0)
    def consumer(bootstrap: String, index: Int): KafkaConsumer[Array[Byte], Array[Byte]] =
      if persistent && clients(index) != null then return clients(index)
      val properties = Properties()
      properties.setProperty("bootstrap.servers", bootstrap)
      properties.setProperty("group.id", s"scale-group-$index")
      properties.setProperty("group.protocol", "classic")
      properties.setProperty("enable.auto.commit", "false")
      properties.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
      properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
      properties.setProperty("default.api.timeout.ms", "30000")
      properties.setProperty("request.timeout.ms", "5000")
      properties.setProperty("enable.metrics.push", "false")
      if persistent then properties.setProperty("connections.max.idle.ms", "3600000"): Unit
      val client = KafkaConsumer[Array[Byte], Array[Byte]](properties)
      clientsCreated.incrementAndGet(): Unit
      if persistent then clients(index) = client
      client
    def parallel(action: Int => Unit): Unit =
      val futures = (0 until groupCount).map { index =>
        executor.submit(new Callable[Unit]:
          override def call(): Unit = action(index)
        )
      }
      futures.foreach(_.get(120L, TimeUnit.SECONDS))
    def write(bootstrap: String, phase: Int, count: Int): Unit =
      val started = System.nanoTime()
      parallel { index =>
        val client = consumer(bootstrap, index)
        try
          client.assign(java.util.List.of(partition))
          (1 to count).foreach { round =>
            val begin = System.nanoTime()
            client.commitSync(Map(partition -> OffsetAndMetadata(index.toLong * 1000L + phase * 100L + round)).asJava)
            latencies.add(System.nanoTime() - begin): Unit
          }
        finally if !persistent then client.close()
      }
      writeNanos += System.nanoTime() - started
    def verify(bootstrap: String, phase: Int, round: Int): Unit = parallel { index =>
      val client = consumer(bootstrap, index)
      try
        val actual = Option(client.committed(java.util.Set.of(partition)).get(partition)).map(_.offset())
        require(actual.contains(index.toLong * 1000L + phase * 100L + round), s"wrong recovered offset for group $index: $actual")
      finally if !persistent then client.close()
    }
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      if persistent then
        enter("warmup")
        write(cluster.bootstrapServers, 0, 1)
        warmupSeconds = writeNanos / 1e9
        writeNanos = 0L
        latencies.clear()
      enter("initial-writes")
      write(cluster.bootstrapServers, 0, rounds)
      enter("initial-verification")
      verify(cluster.bootstrapServers, 0, rounds)
      val initialMetrics = cluster.nodes.map(n => n.id -> cluster.broker(n.id).metricsSnapshot).toMap
      val owners = initialMetrics.collect { case (id, metrics) if metrics.coordinator.attempts > 0L => id }.toVector.sorted
      if groupCount >= 30 then require(owners.size == 3, s"not every broker served coordinator writes: $owners")
      enter("controller-stop")
      cluster.stop(controller.id)
      val survivors = cluster.nodes.filterNot(_.id == controller.id)
      CoordinatorProbe.controller(survivors, Set(controller.id))
      val bootstrap = survivors.map(n => s"${n.host}:${n.port}").mkString(",")
      enter("failover-verification")
      verify(bootstrap, 0, rounds)
      enter("failover-writes")
      write(bootstrap, 1, 1)
      enter("final-verification")
      verify(bootstrap, 1, 1)
      val metrics = cluster.nodes.map { node =>
        if node.id == controller.id then initialMetrics(node.id) else cluster.broker(node.id).metricsSnapshot
      }
      val coordinatorMetrics = metrics.map(_.coordinator)
      val admissionRejections = metrics.map(_.rejectedConnections).sum
      require(admissionRejections == 0L, s"coordinator capacity campaign hit connection admission: $admissionRejections")
      require(metrics.map(_.metadataJournal.deltaBytes).sum > 0L, "incremental journal path was not exercised")
      require(metrics.map(_.metadataTransfers.deltaBytes).sum > 0L, "incremental replication path was not exercised")
      // Restart every process from its existing disk, including the controller that missed the last phase.
      enter("full-restart")
      survivors.foreach(n => cluster.stop(n.id))
      cluster.startAll()
      CoordinatorProbe.controller(cluster.nodes)
      enter("restart-verification")
      verify(cluster.bootstrapServers, 1, 1)
      require(cluster.nodes.forall(n => cluster.broker(n.id).metricsSnapshot.rejectedConnections == 0L),
        "restart recovery hit connection admission")
      val sorted = latencies.asScala.toVector.sorted
      def percentile(p: Double): Double = sorted(math.min(sorted.size - 1, math.ceil(sorted.size * p).toInt - 1)) / 1000000d
      CoordinatorScaleReport(groupCount, concurrency, rounds, groupCount, groupCount * (rounds + 1), writeNanos / 1e9,
        percentile(0.50), percentile(0.95), percentile(0.99), coordinatorMetrics.map(_.attempts).sum, coordinatorMetrics.map(_.failures).sum,
        coordinatorMetrics.map(_.deltaBytes).sum, coordinatorMetrics.map(_.fullImageBytes).sum, owners, true, true, revision, startedAt,
        metrics.map(_.metadataJournal.deltaBytes).sum, metrics.map(_.metadataJournal.fullBytes).sum,
        metrics.map(_.metadataJournal.checkpointBytes).sum, metrics.map(_.metadataTransfers.deltaBytes).sum,
        metrics.map(_.metadataTransfers.fullBytes).sum, metrics.map(_.metadataTransfers.fallbacks).sum,
        if persistent then "persistent" else "churn", clientsCreated.get(), warmupSeconds,
        metrics.map(_.shardObjects.writtenBytes).sum, metrics.map(_.shardObjects.writtenObjects).sum,
        metrics.map(_.shardObjects.reusedObjects).sum, metrics.map(_.shardObjects.reclaimedBytes).sum,
        metrics.map(_.shardObjects.liveBytes).sum, perIpLimit, admissionRejections)
    catch
      case scala.util.control.NonFatal(error) =>
        System.err.println(s"COORDINATOR_SCALE_FAILURE phase=$phase")
        cluster.nodes.filter(n => cluster.runningNodeIds(n.id)).foreach { node =>
          try
            val (term, leader, metadata) = CoordinatorProbe.snapshot(node)
            val health = cluster.broker(node.id).metricsSnapshot
            System.err.println(s"COORDINATOR_SCALE_NODE id=${node.id} term=$term leader=$leader version=${metadata.version} unavailable=${metadata.unavailableBrokerIds.toVector.sorted.mkString(",")} fenced=${health.brokerFenced} active_connections=${health.activeConnections} rejected_connections=${health.rejectedConnections}")
          catch case scala.util.control.NonFatal(probeError) =>
            System.err.println(s"COORDINATOR_SCALE_NODE id=${node.id} snapshot_error=${probeError.getClass.getSimpleName}")
        }
        throw error
    finally
      executor.shutdownNow()
      executor.awaitTermination(10L, TimeUnit.SECONDS)
      try clients.iterator.filter(_ != null).foreach { client =>
        try client.close(org.apache.kafka.clients.consumer.CloseOptions.timeout(java.time.Duration.ofSeconds(2L)))
        catch case scala.util.control.NonFatal(error) => System.err.println(s"benchmark client cleanup failed: ${error.getMessage}")
      }
      finally cluster.close()

  private def currentRevision(): String =
    def git(arguments: String*): String =
      val process = ProcessBuilder((Vector("git") ++ arguments).asJava).redirectErrorStream(true).start()
      if !process.waitFor(5L, TimeUnit.SECONDS) then
        process.destroyForcibly()
        throw IllegalStateException("git revision lookup timed out")
      val result = String(process.getInputStream.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8).trim
      if process.exitValue() != 0 then throw IllegalStateException("git revision lookup failed")
      result
    val revision = git("rev-parse", "HEAD")
    require(revision.matches("[0-9a-f]{40}"), "invalid revision")
    revision + (if git("status", "--porcelain").nonEmpty then "+working-tree" else "")

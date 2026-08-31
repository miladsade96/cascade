package cascade.qualification

import cascade.broker.{BrokerConfig, KafkaBroker}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final case class SoakConfig(
    durationSeconds: Long = 72L * 60 * 60,
    tenants: Int = 8,
    recordsPerCycle: Int = 100,
    payloadBytes: Int = 512,
    reportIntervalSeconds: Long = 60L,
    keepData: Boolean = false,
    reportPath: Option[Path] = None
):
  require(durationSeconds > 0L, "soak duration must be positive")
  require(tenants > 0 && tenants <= 1000, "tenant count must be between 1 and 1000")
  require(recordsPerCycle > 0, "records per cycle must be positive")
  require(payloadBytes >= 32, "payload must be at least 32 bytes")
  require(reportIntervalSeconds > 0L, "report interval must be positive")

object SoakConfig:
  def parse(arguments: Array[String]): SoakConfig =
    @annotation.tailrec
    def loop(values: List[String], config: SoakConfig): SoakConfig = values match
      case Nil => config
      case "--duration-hours" :: value :: tail => loop(tail, config.copy(durationSeconds = math.ceil(value.toDouble * 3600d).toLong))
      case "--duration-seconds" :: value :: tail => loop(tail, config.copy(durationSeconds = value.toLong))
      case "--tenants" :: value :: tail => loop(tail, config.copy(tenants = value.toInt))
      case "--records-per-cycle" :: value :: tail => loop(tail, config.copy(recordsPerCycle = value.toInt))
      case "--payload-bytes" :: value :: tail => loop(tail, config.copy(payloadBytes = value.toInt))
      case "--report-interval-seconds" :: value :: tail => loop(tail, config.copy(reportIntervalSeconds = value.toLong))
      case "--report" :: value :: tail => loop(tail, config.copy(reportPath = Some(Paths.get(value))))
      case "--keep-data" :: tail => loop(tail, config.copy(keepData = true))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete soak option: $option")
    loop(arguments.toList, SoakConfig())

final case class SoakReport(
    elapsedSeconds: Double,
    records: Long,
    cycles: Long,
    tenants: Int,
    peakHeapBytes: Long,
    mismatches: Long
):
  def json: String =
    f"""{"status":"${if mismatches == 0 then "passed" else "failed"}","elapsed_seconds":$elapsedSeconds%.3f,"records":$records,"cycles":$cycles,"tenants":$tenants,"peak_heap_bytes":$peakHeapBytes,"mismatches":$mismatches}"""

object SoakTest:
  def main(arguments: Array[String]): Unit =
    val report = run(SoakConfig.parse(arguments))
    println(s"SOAK_RESULT ${report.json}")
    if report.mismatches != 0L then throw IllegalStateException(s"soak detected ${report.mismatches} mismatches")

  def run(config: SoakConfig): SoakReport =
    val dataDirectory = Files.createTempDirectory("cascade-multitenant-soak")
    val broker = KafkaBroker(BrokerConfig(
      bindHost = "127.0.0.1",
      port = 0,
      advertisedHost = "127.0.0.1",
      dataDirectory = dataDirectory,
      autoCreateTopics = false
    ))
    var producers = Vector.empty[KafkaProducer[Array[Byte], Array[Byte]]]
    var consumers = Vector.empty[KafkaConsumer[Array[Byte], Array[Byte]]]
    val memory = java.lang.management.ManagementFactory.getMemoryMXBean
    try
      broker.start()
      val topics = Vector.tabulate(config.tenants)(index => s"soak-tenant-$index")
      val admin = Admin.create(properties(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> broker.bootstrapServers))
      try admin.createTopics(topics.map(name => NewTopic(name, 1, 1.toShort)).asJava).all().get(30, TimeUnit.SECONDS)
      finally admin.close(Duration.ofSeconds(5))
      producers = topics.map(topic => KafkaProducer(producerProperties(broker.bootstrapServers, topic)))
      consumers = topics.map { topic =>
        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(broker.bootstrapServers, topic))
        val partition = TopicPartition(topic, 0)
        consumer.assign(java.util.List.of(partition))
        consumer.seekToBeginning(java.util.List.of(partition))
        consumer
      }
      val started = System.nanoTime()
      val deadline = started + Duration.ofSeconds(config.durationSeconds).toNanos
      var nextReport = started + Duration.ofSeconds(config.reportIntervalSeconds).toNanos
      val sequence = Array.fill[Long](config.tenants)(0L)
      var records = 0L
      var cycles = 0L
      var mismatches = 0L
      var peakHeap = 0L
      while System.nanoTime() < deadline do
        topics.indices.foreach { tenant =>
          val producer = producers(tenant)
          val first = sequence(tenant)
          (0 until config.recordsPerCycle).foreach { index =>
            val current = first + index
            producer.send(ProducerRecord(topics(tenant), 0, longBytes(current), payload(tenant, current, config.payloadBytes)))
          }
          producer.flush()
          val target = first + config.recordsPerCycle
          val consumer = consumers(tenant)
          val readDeadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
          while sequence(tenant) < target && System.nanoTime() < readDeadline do
            consumer.poll(Duration.ofMillis(100)).iterator().asScala.foreach { record =>
              val expected = sequence(tenant)
              if record.offset() != expected || !java.util.Arrays.equals(record.value(), payload(tenant, expected, config.payloadBytes)) then
                mismatches += 1L
              sequence(tenant) += 1L
              records += 1L
            }
          if sequence(tenant) != target then throw IllegalStateException(s"tenant $tenant stalled at ${sequence(tenant)}/$target")
        }
        cycles += 1L
        peakHeap = math.max(peakHeap, memory.getHeapMemoryUsage.getUsed)
        if System.nanoTime() >= nextReport then
          val elapsed = (System.nanoTime() - started) / 1_000_000_000d
          println(f"SOAK_PROGRESS elapsed_seconds=$elapsed%.1f records=$records cycles=$cycles mismatches=$mismatches peak_heap_bytes=$peakHeap")
          nextReport = System.nanoTime() + Duration.ofSeconds(config.reportIntervalSeconds).toNanos
      val report = SoakReport((System.nanoTime() - started) / 1_000_000_000d, records, cycles, config.tenants, peakHeap, mismatches)
      config.reportPath.foreach { path =>
        Option(path.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
        Files.writeString(path, report.json + System.lineSeparator(), StandardCharsets.UTF_8): Unit
      }
      report
    finally
      consumers.foreach(_.close())
      producers.foreach(_.close(Duration.ofSeconds(5)))
      broker.close()
      if config.keepData then println(s"SOAK_DATA_DIRECTORY ${dataDirectory.toAbsolutePath}") else deleteTree(dataDirectory)

  private def producerProperties(bootstrap: String, tenant: String): Properties = properties(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.ACKS_CONFIG -> "all",
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "true",
    ProducerConfig.CLIENT_ID_CONFIG -> tenant,
    "enable.metrics.push" -> "false"
  )

  private def consumerProperties(bootstrap: String, tenant: String): Properties = properties(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
    ConsumerConfig.CLIENT_ID_CONFIG -> tenant,
    "enable.metrics.push" -> "false"
  )

  private def properties(values: (String, String)*): Properties =
    val result = Properties()
    values.foreach { case (name, value) => result.put(name, value): Unit }
    result

  private def longBytes(value: Long): Array[Byte] = java.nio.ByteBuffer.allocate(8).putLong(value).array()

  private def payload(tenant: Int, sequence: Long, bytes: Int): Array[Byte] =
    val value = new Array[Byte](bytes)
    val header = java.nio.ByteBuffer.wrap(value)
    header.putInt(tenant).putLong(sequence)
    var index = 12
    while index < value.length do
      value(index) = ((tenant * 31L + sequence * 17L + index) & 0xff).toByte
      index += 1
    value

  private def deleteTree(root: Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

final class SoakTestSuite extends munit.FunSuite:
  test("multi-tenant soak harness completes an exact smoke interval") {
    val report = SoakTest.run(SoakConfig(durationSeconds = 1L, tenants = 2, recordsPerCycle = 10, payloadBytes = 64))
    assert(report.records >= 20L)
    assertEquals(report.mismatches, 0L)
  }

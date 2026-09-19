package cascade.performance

import java.lang.management.ManagementFactory
import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.{Arrays, Properties}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final case class KafkaComparisonConfig(
    engine: String,
    bootstrapServers: String,
    topic: String,
    records: Int = 250_000,
    warmupRecords: Int = 25_000,
    payloadBytes: Int = 1024,
    partitions: Int = 8,
    replicationFactor: Short = 1,
    producers: Int = 4,
    compression: String = "lz4",
    acks: String = "all",
    output: Option[Path] = None
):
  require(engine.matches("[A-Za-z0-9._-]+"), "engine must be a short identifier")
  require(records > 0 && warmupRecords >= 0, "record counts must be valid")
  require(payloadBytes >= java.lang.Long.BYTES, "payload must hold the verification index")
  require(partitions > 0 && replicationFactor > 0 && producers > 0, "topology values must be positive")

object KafkaComparisonConfig:
  def parse(arguments: Array[String]): KafkaComparisonConfig =
    @annotation.tailrec
    def loop(remaining: List[String], config: Option[KafkaComparisonConfig]): KafkaComparisonConfig = remaining match
      case Nil => config.getOrElse(throw IllegalArgumentException("--engine and --bootstrap are required"))
      case "--engine" :: engine :: "--bootstrap" :: bootstrap :: tail =>
        loop(tail, Some(KafkaComparisonConfig(engine, bootstrap, s"cascade-comparison-${engine.toLowerCase}")))
      case "--records" :: value :: tail => loop(tail, Some(required(config).copy(records = value.toInt)))
      case "--warmup-records" :: value :: tail => loop(tail, Some(required(config).copy(warmupRecords = value.toInt)))
      case "--payload-bytes" :: value :: tail => loop(tail, Some(required(config).copy(payloadBytes = value.toInt)))
      case "--partitions" :: value :: tail => loop(tail, Some(required(config).copy(partitions = value.toInt)))
      case "--replication-factor" :: value :: tail => loop(tail, Some(required(config).copy(replicationFactor = value.toShort)))
      case "--producers" :: value :: tail => loop(tail, Some(required(config).copy(producers = value.toInt)))
      case "--compression" :: value :: tail => loop(tail, Some(required(config).copy(compression = value)))
      case "--acks" :: value :: tail => loop(tail, Some(required(config).copy(acks = value)))
      case "--topic" :: value :: tail => loop(tail, Some(required(config).copy(topic = value)))
      case "--output" :: value :: tail => loop(tail, Some(required(config).copy(output = Some(Path.of(value)))))
      case option :: _ => throw IllegalArgumentException(s"unknown, incomplete, or out-of-order option: $option")
    loop(arguments.toList, None)

  private def required(config: Option[KafkaComparisonConfig]): KafkaComparisonConfig =
    config.getOrElse(throw IllegalArgumentException("--engine and --bootstrap must be the first options"))

final case class KafkaComparisonResult(
    engine: String,
    records: Int,
    warmupRecords: Int,
    payloadBytes: Int,
    partitions: Int,
    replicationFactor: Short,
    producers: Int,
    compression: String,
    acks: String,
    warmupMillis: Long,
    produceMillis: Long,
    consumeMillis: Long,
    produceRecordsPerSecond: Double,
    consumeRecordsPerSecond: Double,
    p50Micros: Long,
    p95Micros: Long,
    p99Micros: Long,
    consumed: Int,
    lost: Int,
    unexpectedDuplicates: Int,
    clientHeapMebibytes: Double,
    jdk: String,
    scala: String,
    os: String,
    processors: Int
):
  def json: String =
    s"""{"engine":"$engine","records":$records,"warmup_records":$warmupRecords,"payload_bytes":$payloadBytes,"partitions":$partitions,"replication_factor":$replicationFactor,"producers":$producers,"compression":"$compression","acks":"$acks","warmup_ms":$warmupMillis,"produce_ms":$produceMillis,"consume_ms":$consumeMillis,"produce_records_per_second":${format(produceRecordsPerSecond)},"consume_records_per_second":${format(consumeRecordsPerSecond)},"ack_p50_us":$p50Micros,"ack_p95_us":$p95Micros,"ack_p99_us":$p99Micros,"consumed":$consumed,"lost":$lost,"unexpected_duplicates":$unexpectedDuplicates,"client_heap_mib":${format(clientHeapMebibytes)},"jdk":"$jdk","scala":"$scala","os":"$os","processors":$processors}"""

  private def format(value: Double): String = java.lang.String.format(java.util.Locale.ROOT, "%.3f", value)

object KafkaComparisonBenchmark:
  def main(arguments: Array[String]): Unit =
    val config = KafkaComparisonConfig.parse(arguments)
    val result = run(config)
    config.output.foreach { path =>
      Option(path.getParent).foreach(parent => Files.createDirectories(parent): Unit)
      Files.writeString(path, result.json + System.lineSeparator())
    }
    println(s"KAFKA_COMPARISON_RESULT ${result.json}")

  def run(config: KafkaComparisonConfig): KafkaComparisonResult =
    val admin = Admin.create(adminProperties(config.bootstrapServers))
    try
      admin.createTopics(java.util.List.of(NewTopic(config.topic, config.partitions, config.replicationFactor)))
        .all().get(60, TimeUnit.SECONDS)
    finally admin.close(Duration.ofSeconds(5))

    val warmupStarted = System.nanoTime()
    produce(config, config.warmupRecords, measureLatency = false)
    val warmupMillis = nanosToMillis(System.nanoTime() - warmupStarted)

    val produced = produce(config, config.records, measureLatency = true)
    val consumed = consume(config)
    if consumed._1 != config.records || consumed._2 != 0 then
      throw IllegalStateException(s"exact verification failed: consumed=${consumed._1} duplicates=${consumed._2}")
    val memory = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed / 1024.0 / 1024.0
    KafkaComparisonResult(
      config.engine,
      config.records,
      config.warmupRecords,
      config.payloadBytes,
      config.partitions,
      config.replicationFactor,
      config.producers,
      config.compression,
      config.acks,
      warmupMillis,
      produced.elapsedMillis,
      consumed._3,
      perSecond(config.records, produced.elapsedMillis),
      perSecond(config.records, consumed._3),
      percentile(produced.latenciesMicros, 0.50),
      percentile(produced.latenciesMicros, 0.95),
      percentile(produced.latenciesMicros, 0.99),
      consumed._1,
      config.records - consumed._1,
      consumed._2,
      memory,
      System.getProperty("java.version"),
      scala.util.Properties.versionNumberString,
      s"${System.getProperty("os.name")} ${System.getProperty("os.version")} ${System.getProperty("os.arch")}",
      Runtime.getRuntime.availableProcessors()
    )

  private final case class ProduceMeasurement(elapsedMillis: Long, latenciesMicros: Array[Long])

  private def produce(config: KafkaComparisonConfig, records: Int, measureLatency: Boolean): ProduceMeasurement =
    if records == 0 then return ProduceMeasurement(0L, Array.emptyLongArray)
    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(config))
    val latch = CountDownLatch(records)
    val failure = AtomicReference[Throwable]()
    val latencies = if measureLatency then Array.ofDim[Long](records) else Array.emptyLongArray
    val latencyIndex = AtomicInteger(0)
    val started = System.nanoTime()
    try
      var index = 0
      while index < records do
        val recordIndex = if measureLatency then index.toLong else -index.toLong - 1L
        val payload = payloadFor(recordIndex, config.payloadBytes)
        val sentAt = System.nanoTime()
        producer.send(
          new ProducerRecord[Array[Byte], Array[Byte]](config.topic, index % config.partitions, null, payload),
          (_, error) =>
            if error != null then failure.compareAndSet(null, error): Unit
            else if measureLatency then
              val slot = latencyIndex.getAndIncrement()
              latencies(slot) = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - sentAt)
            latch.countDown()
        )
        index += 1
      producer.flush()
      if !latch.await(60, TimeUnit.SECONDS) then throw IllegalStateException(s"timed out with ${latch.getCount} unacknowledged records")
      Option(failure.get()).foreach(throw _)
      ProduceMeasurement(nanosToMillis(System.nanoTime() - started), latencies)
    finally producer.close(Duration.ofSeconds(30))

  private def consume(config: KafkaComparisonConfig): (Int, Int, Long) =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(config.bootstrapServers))
    val partitions = (0 until config.partitions).map(TopicPartition(config.topic, _)).toVector
    val seen = Array.fill(config.records)(false)
    val warmupPerPartition = Array.fill(config.partitions)(0L)
    var warmupIndex = 0
    while warmupIndex < config.warmupRecords do
      warmupPerPartition(warmupIndex % config.partitions) += 1L
      warmupIndex += 1
    var unique = 0
    var duplicates = 0
    val started = System.nanoTime()
    val deadline = started + TimeUnit.SECONDS.toNanos(120)
    try
      consumer.assign(partitions.asJava)
      partitions.foreach(partition => consumer.seek(partition, warmupPerPartition(partition.partition())))
      while unique < config.records && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(250)).iterator().asScala.foreach { record =>
          val index = ByteBuffer.wrap(record.value()).getLong()
          if index < 0L || index >= config.records then throw IllegalStateException(s"unexpected measured index $index")
          if seen(index.toInt) then duplicates += 1
          else
            seen(index.toInt) = true
            unique += 1
        }
      (unique, duplicates, nanosToMillis(System.nanoTime() - started))
    finally consumer.close()

  private def payloadFor(index: Long, size: Int): Array[Byte] =
    val bytes = Array.ofDim[Byte](size)
    ByteBuffer.wrap(bytes).putLong(index)
    var state = index ^ 0x9e3779b97f4a7c15L
    var offset = java.lang.Long.BYTES
    while offset < size do
      state ^= state << 13
      state ^= state >>> 7
      state ^= state << 17
      bytes(offset) = state.toByte
      offset += 1
    bytes

  private def percentile(values: Array[Long], fraction: Double): Long =
    if values.isEmpty then 0L
    else
      val copy = values.clone()
      Arrays.sort(copy)
      copy(math.min(copy.length - 1, math.ceil(copy.length * fraction).toInt - 1))

  private def nanosToMillis(nanos: Long): Long = math.max(1L, TimeUnit.NANOSECONDS.toMillis(nanos))
  private def perSecond(records: Int, millis: Long): Double = records.toDouble * 1000.0 / millis.toDouble

  private def adminProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "60000")
    properties

  private def producerProperties(config: KafkaComparisonConfig): Properties =
    val properties = Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, config.acks)
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compression)
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, (128 * 1024).toString)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000")
    properties

  private def consumerProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (64 * 1024 * 1024).toString)
    properties


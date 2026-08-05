package cascade.performance

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.storage.FlushPolicy
import com.sun.management.OperatingSystemMXBean
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.{Properties, Random}
import java.util.concurrent.atomic.{AtomicLong, AtomicLongArray, AtomicReference}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final case class LoadConfig(
    records: Int = 1_000_000,
    payloadBytes: Int = 1024,
    partitions: Int = 8,
    producers: Int = 4,
    consumers: Int = 4,
    compression: String = "lz4",
    segmentBytes: Long = 256L * 1024 * 1024,
    flushPolicy: FlushPolicy = FlushPolicy.Periodic,
    flushIntervalMillis: Long = 1000L,
    flushBytes: Long = 64L * 1024 * 1024,
    keepData: Boolean = false
):
  require(records > 0, "records must be positive")
  require(payloadBytes > 0, "payload bytes must be positive")
  require(partitions > 0, "partitions must be positive")
  require(producers > 0, "producers must be positive")
  require(consumers > 0, "consumers must be positive")
  require(flushIntervalMillis > 0, "flush interval must be positive")
  require(flushBytes > 0, "flush bytes must be positive")

object LoadConfig:
  def parse(arguments: Array[String]): LoadConfig =
    @annotation.tailrec
    def loop(remaining: List[String], config: LoadConfig): LoadConfig = remaining match
      case Nil => config
      case "--records" :: value :: tail => loop(tail, config.copy(records = value.toInt))
      case "--payload-bytes" :: value :: tail => loop(tail, config.copy(payloadBytes = value.toInt))
      case "--partitions" :: value :: tail => loop(tail, config.copy(partitions = value.toInt))
      case "--producers" :: value :: tail => loop(tail, config.copy(producers = value.toInt))
      case "--consumers" :: value :: tail => loop(tail, config.copy(consumers = value.toInt))
      case "--compression" :: value :: tail => loop(tail, config.copy(compression = value))
      case "--segment-bytes" :: value :: tail => loop(tail, config.copy(segmentBytes = value.toLong))
      case "--flush-policy" :: value :: tail => loop(tail, config.copy(flushPolicy = FlushPolicy.parse(value)))
      case "--flush-interval-ms" :: value :: tail => loop(tail, config.copy(flushIntervalMillis = value.toLong))
      case "--flush-bytes" :: value :: tail => loop(tail, config.copy(flushBytes = value.toLong))
      case "--keep-data" :: tail => loop(tail, config.copy(keepData = true))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete option: $option")
    loop(arguments.toList, LoadConfig())

private final case class RuntimeSnapshot(cpuNanos: Long, gcCount: Long, gcMillis: Long)

private final case class PhaseResult(
    elapsedSeconds: Double,
    recordsPerSecond: Double,
    mebibytesPerSecond: Double,
    cpuCores: Double,
    machineCpuPercent: Double,
    gcCollections: Long,
    gcMillis: Long
)

private final class LatencyHistogram:
  private val upperBoundsMicros = Array[Long](250, 500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000, 5_000_000, Long.MaxValue)
  private val counts = AtomicLongArray(upperBoundsMicros.length)
  private val total = AtomicLong(0)
  private val maximum = AtomicLong(0)

  def record(nanos: Long): Unit =
    val micros = math.max(0L, nanos / 1_000L)
    var index = 0
    while upperBoundsMicros(index) < micros do index += 1
    counts.incrementAndGet(index)
    total.incrementAndGet()
    maximum.accumulateAndGet(micros, Math.max): Unit

  def percentile(fraction: Double): String =
    val target = math.max(1L, math.ceil(total.get() * fraction).toLong)
    var cumulative = 0L
    var index = 0
    while index < counts.length() do
      cumulative += counts.get(index)
      if cumulative >= target then return formatBound(upperBoundsMicros(index))
      index += 1
    "n/a"

  def maxMillis: Double = maximum.get() / 1000.0

  private def formatBound(micros: Long): String =
    if micros == Long.MaxValue then ">5000ms"
    else if micros < 1000 then s"<=${micros}us"
    else s"<=${micros / 1000.0}ms"

private final class HeapSampler extends AutoCloseable:
  private val peakBytes = AtomicLong(0)
  private val running = AtomicLong(1)
  private val thread = Thread.ofPlatform().daemon().name("cascade-load-heap-sampler").start { () =>
    val memory = ManagementFactory.getMemoryMXBean
    while running.get() == 1 do
      peakBytes.accumulateAndGet(memory.getHeapMemoryUsage.getUsed, Math.max)
      Thread.sleep(50)
  }

  def peakMebibytes: Double = peakBytes.get() / 1024.0 / 1024.0

  override def close(): Unit =
    running.set(0)
    thread.join(1000)

object LoadTest:
  private val Topic = "cascade-load"

  def main(arguments: Array[String]): Unit =
    val config = LoadConfig.parse(arguments)
    val dataDirectory = Files.createTempDirectory("cascade-heavy-load")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = dataDirectory,
        maxRequestBytes = 128 * 1024 * 1024,
        segmentBytes = config.segmentBytes,
        flushPolicy = config.flushPolicy,
        flushIntervalMillis = config.flushIntervalMillis,
        flushBytes = config.flushBytes,
        autoCreateTopics = false
      )
    )
    val heapSampler = HeapSampler()
    try
      broker.start()
      createTopic(broker.bootstrapServers, config.partitions)
      println(s"LOAD_CONFIG records=${config.records} payload_bytes=${config.payloadBytes} partitions=${config.partitions} producers=${config.producers} consumers=${config.consumers} compression=${config.compression} flush_policy=${config.flushPolicy.toString.toLowerCase(java.util.Locale.ROOT)} flush_interval_ms=${config.flushIntervalMillis} flush_bytes=${config.flushBytes}")

      val latency = LatencyHistogram()
      val produceResult = measure(config.records, config.payloadBytes) {
        produce(broker.bootstrapServers, config, latency)
      }
      val storedBytes = directoryBytes(dataDirectory)
      println(f"PRODUCE records_per_second=${produceResult.recordsPerSecond}%.0f mebibytes_per_second=${produceResult.mebibytesPerSecond}%.1f elapsed_seconds=${produceResult.elapsedSeconds}%.3f cpu_cores=${produceResult.cpuCores}%.2f machine_cpu_percent=${produceResult.machineCpuPercent}%.1f gc_collections=${produceResult.gcCollections}%d gc_millis=${produceResult.gcMillis}%d")
      println(f"ACK_LATENCY p50=${latency.percentile(0.50)} p95=${latency.percentile(0.95)} p99=${latency.percentile(0.99)} p999=${latency.percentile(0.999)} max_ms=${latency.maxMillis}%.3f")
      println(f"STORAGE bytes=$storedBytes%d mebibytes=${storedBytes / 1024.0 / 1024.0}%.1f bytes_per_record=${storedBytes.toDouble / config.records}%.1f")
      val flush = broker.flushStatistics
      println(f"FLUSH force_operations=${flush.forces}%d forced_mebibytes=${flush.bytes / 1024.0 / 1024.0}%.1f force_millis=${flush.nanos / 1_000_000.0}%.1f pending_mebibytes=${flush.pendingBytes / 1024.0 / 1024.0}%.1f")

      val consumed = AtomicLong(0)
      val consumeResult = measure(config.records, config.payloadBytes) {
        consumed.set(consume(broker.bootstrapServers, config))
      }
      if consumed.get() != config.records then
        throw IllegalStateException(s"consumer verification failed: expected ${config.records}, got ${consumed.get()}")
      println(f"CONSUME records=${consumed.get()}%d records_per_second=${consumeResult.recordsPerSecond}%.0f mebibytes_per_second=${consumeResult.mebibytesPerSecond}%.1f elapsed_seconds=${consumeResult.elapsedSeconds}%.3f cpu_cores=${consumeResult.cpuCores}%.2f machine_cpu_percent=${consumeResult.machineCpuPercent}%.1f gc_collections=${consumeResult.gcCollections}%d gc_millis=${consumeResult.gcMillis}%d")
    finally
      println(f"MEMORY peak_heap_mebibytes=${heapSampler.peakMebibytes}%.1f")
      heapSampler.close()
      broker.close()
      if config.keepData then println(s"DATA_DIRECTORY ${dataDirectory.toAbsolutePath}")
      else deleteTree(dataDirectory)

  private def createTopic(bootstrapServers: String, partitions: Int): Unit =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    val admin = Admin.create(properties)
    try admin.createTopics(java.util.List.of(new NewTopic(Topic, partitions, 1.toShort))).all().get(30, TimeUnit.SECONDS): Unit
    finally admin.close(Duration.ofSeconds(5))

  private def produce(bootstrapServers: String, config: LoadConfig, latency: LatencyHistogram): Unit =
    val payloads = payloadPool(config.payloadBytes)
    val acknowledgements = CountDownLatch(config.records)
    val firstFailure = AtomicReference[Throwable]()
    val executor = Executors.newFixedThreadPool(config.producers)
    try
      val tasks = (0 until config.producers).map { producerIndex =>
        executor.submit(new Callable[Unit]:
          override def call(): Unit =
            val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrapServers, config.compression))
            try
              var recordIndex = producerIndex
              while recordIndex < config.records do
                val started = System.nanoTime()
                val record = new ProducerRecord[Array[Byte], Array[Byte]](
                  Topic,
                  recordIndex % config.partitions,
                  null,
                  payloads(recordIndex & (payloads.length - 1))
                )
                producer.send(record, (_, error) =>
                  latency.record(System.nanoTime() - started)
                  if error != null then firstFailure.compareAndSet(null, error): Unit
                  acknowledgements.countDown()
                )
                recordIndex += config.producers
              producer.flush()
            finally producer.close(Duration.ofSeconds(30))
        )
      }
      tasks.foreach(_.get(10, TimeUnit.MINUTES))
      if !acknowledgements.await(1, TimeUnit.MINUTES) then
        throw IllegalStateException(s"timed out with ${acknowledgements.getCount} unacknowledged records")
      Option(firstFailure.get()).foreach(throw _)
    finally
      executor.shutdownNow(): Unit
      executor.awaitTermination(30, TimeUnit.SECONDS): Unit

  private def consume(bootstrapServers: String, config: LoadConfig): Long =
    val workerCount = math.min(config.consumers, config.partitions)
    val executor = Executors.newFixedThreadPool(workerCount)
    try
      val tasks = (0 until workerCount).map { workerIndex =>
        executor.submit(new Callable[Long]:
          override def call(): Long =
            val assigned = (0 until config.partitions)
              .filter(_ % workerCount == workerIndex)
              .map(partition => new TopicPartition(Topic, partition))
              .toVector
            val expected = assigned.map(partition => recordsInPartition(config.records, config.partitions, partition.partition())).sum
            val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrapServers))
            try
              consumer.assign(assigned.asJava)
              consumer.seekToBeginning(assigned.asJava)
              var received = 0L
              var lastProgress = System.nanoTime()
              while received < expected do
                val records = consumer.poll(Duration.ofMillis(250))
                if !records.isEmpty then
                  received += records.count()
                  lastProgress = System.nanoTime()
                else if System.nanoTime() - lastProgress > Duration.ofSeconds(30).toNanos then
                  val positions = assigned.map(partition => s"$partition=${consumer.position(partition)}").mkString(",")
                  val endOffsets = consumer.endOffsets(assigned.asJava).asScala.toVector
                    .sortBy(_._1.partition())
                    .map { case (partition, offset) => s"$partition=$offset" }
                    .mkString(",")
                  throw IllegalStateException(
                    s"consumer $workerIndex stalled at $received/$expected; positions=[$positions]; end_offsets=[$endOffsets]"
                  )
              received
            finally consumer.close()
        )
      }
      tasks.map(_.get(10, TimeUnit.MINUTES)).sum
    finally
      executor.shutdownNow(): Unit
      executor.awaitTermination(30, TimeUnit.SECONDS): Unit

  private def producerProperties(bootstrapServers: String, compression: String): Properties =
    val properties = Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, (128 * 1024).toString)
    properties.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, (256L * 1024 * 1024).toString)
    properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "300000")
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "300000")
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000")
    properties.put("enable.metrics.push", "false")
    properties

  private def consumerProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10000")
    properties.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, (64 * 1024 * 1024).toString)
    properties.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, (8 * 1024 * 1024).toString)
    properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100")
    properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "300000")
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "120000")
    properties.put("enable.metrics.push", "false")
    properties

  private def payloadPool(payloadBytes: Int): Array[Array[Byte]] =
    val random = new Random(0xCA5CADEL)
    Array.fill(4096) {
      val payload = new Array[Byte](payloadBytes)
      random.nextBytes(payload)
      payload
    }

  private def recordsInPartition(records: Int, partitions: Int, partition: Int): Long =
    if partition >= records then 0L else ((records - 1L - partition) / partitions) + 1L

  private def measure(records: Int, payloadBytes: Int)(operation: => Unit): PhaseResult =
    val before = snapshot()
    val started = System.nanoTime()
    operation
    val elapsedNanos = System.nanoTime() - started
    val after = snapshot()
    val elapsedSeconds = elapsedNanos / 1_000_000_000.0
    val cpuNanos = after.cpuNanos - before.cpuNanos
    val logicalProcessors = Runtime.getRuntime.availableProcessors()
    val cpuCores = cpuNanos.toDouble / elapsedNanos
    PhaseResult(
      elapsedSeconds = elapsedSeconds,
      recordsPerSecond = records / elapsedSeconds,
      mebibytesPerSecond = records.toDouble * payloadBytes / 1024.0 / 1024.0 / elapsedSeconds,
      cpuCores = cpuCores,
      machineCpuPercent = cpuCores / logicalProcessors * 100.0,
      gcCollections = after.gcCount - before.gcCount,
      gcMillis = after.gcMillis - before.gcMillis
    )

  private def snapshot(): RuntimeSnapshot =
    val operatingSystem = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[OperatingSystemMXBean]
    val collectors = ManagementFactory.getGarbageCollectorMXBeans.asScala
    RuntimeSnapshot(
      cpuNanos = operatingSystem.getProcessCpuTime,
      gcCount = collectors.map(_.getCollectionCount).filter(_ >= 0).sum,
      gcMillis = collectors.map(_.getCollectionTime).filter(_ >= 0).sum
    )

  private def directoryBytes(root: Path): Long =
    val paths = Files.walk(root)
    try paths.iterator().asScala.filter(Files.isRegularFile(_)).map(Files.size).sum
    finally paths.close()

  private def deleteTree(root: Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

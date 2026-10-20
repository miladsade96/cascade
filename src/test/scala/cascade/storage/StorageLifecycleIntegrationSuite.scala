package cascade.storage

import cascade.broker.{BrokerConfig, KafkaBroker}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.{ExecutionException, TimeUnit}
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition as KafkaTopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class StorageLifecycleIntegrationSuite extends FunSuite:
  private final case class ConsumedRecord(offset: Long, key: Option[Vector[Byte]], value: Vector[Byte])

  test("broker size retention advances the Kafka log start and survives restart") {
    val directory = Files.createTempDirectory("cascade-broker-retention-test")
    val config = BrokerConfig(
      bindHost = "127.0.0.1",
      port = 0,
      advertisedHost = "127.0.0.1",
      dataDirectory = directory,
      segmentBytes = 1024L,
      flushPolicy = FlushPolicy.Sync,
      storageLifecycle = StorageLifecycleConfig(
        cleanupPolicy = CleanupPolicy.Delete,
        retentionMillis = -1L,
        retentionBytes = 1024L,
        lifecycleIntervalMillis = 20L
      )
    )
    var broker = KafkaBroker(config)
    try
      broker.start()
      createTopic(broker.bootstrapServers, "retained-events")
      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(broker.bootstrapServers))
      try
        (0 until 40).foreach { index =>
          val metadata = producer.send(
            ProducerRecord("retained-events", 0, null, s"value-$index".getBytes(StandardCharsets.UTF_8))
          ).get(10, TimeUnit.SECONDS)
          assertEquals(metadata.offset(), index.toLong)
        }
      finally producer.close(Duration.ofSeconds(5))

      val deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos
      while broker.lifecycleStatistics.retiredSegments == 0L && System.nanoTime() < deadline do Thread.sleep(10L)
      assert(broker.lifecycleStatistics.retiredSegments > 0L)
      val retained = consumeAvailable(broker.bootstrapServers, "retained-events")
      assert(retained.nonEmpty)
      assert(retained.size < 40)
      assertEquals(retained.last, "value-39")

      broker.close()
      broker = KafkaBroker(config)
      broker.start()
      val restarted = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(broker.bootstrapServers))
      try
        val metadata = restarted.send(
          ProducerRecord("retained-events", 0, null, "after-restart".getBytes(StandardCharsets.UTF_8))
        ).get(10, TimeUnit.SECONDS)
        assertEquals(metadata.offset(), 40L)
      finally restarted.close(Duration.ofSeconds(5))
    finally
      broker.close()
      deleteTree(directory)
  }

  test("broker reports Kafka storage errors when the disk reserve rejects an append") {
    val directory = Files.createTempDirectory("cascade-broker-pressure-test")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory,
        storageLifecycle = StorageLifecycleConfig(minimumFreeBytes = Long.MaxValue / 4L)
      )
    )
    try
      broker.start()
      createTopic(broker.bootstrapServers, "pressure-events")
      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(broker.bootstrapServers))
      try
        intercept[ExecutionException] {
          producer.send(ProducerRecord("pressure-events", "blocked".getBytes(StandardCharsets.UTF_8)))
            .get(10, TimeUnit.SECONDS)
        }
        assertEquals(broker.lifecycleStatistics.rejectedAppends, 1L)
      finally producer.close(Duration.ofSeconds(5))
    finally
      broker.close()
      deleteTree(directory)
  }

  test("Kafka clients read compacted Snappy, LZ4, and Zstd batches with exact keys, values, and offsets") {
    val directory = Files.createTempDirectory("cascade-broker-compressed-compaction-test")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory,
        segmentBytes = 8 * 1024L,
        flushPolicy = FlushPolicy.Sync,
        storageLifecycle = StorageLifecycleConfig(
          cleanupPolicy = CleanupPolicy.Compact,
          retentionMillis = -1L,
          lifecycleIntervalMillis = 20L,
          compactionMaxBytesPerSecond = 100_000_000L
        )
      )
    )
    try
      broker.start()
      Vector("snappy", "lz4", "zstd").zipWithIndex.foreach { case (compression, codecIndex) =>
        val topic = s"compacted-$compression"
        createTopic(broker.bootstrapServers, topic)
        val properties = producerProperties(broker.bootstrapServers)
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compression)
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, (64 * 1024).toString)
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1000")
        val compactedBefore = broker.lifecycleStatistics.compactedBatches
        val producer = KafkaProducer[Array[Byte], Array[Byte]](properties)
        val keys = (0 until 8).map(index => s"key-$index".getBytes(StandardCharsets.UTF_8)).toVector
        val latestValues = keys.indices.map(index => randomBytes(1024, codecIndex * 1000L + index + 100L)).toVector
        try
          val duplicateSends =
            keys.indices.map { index =>
              producer.send(ProducerRecord(topic, 0, keys(index), randomBytes(1024, codecIndex * 1000L + index)))
            } ++ keys.indices.map { index =>
              producer.send(ProducerRecord(topic, 0, keys(index), latestValues(index)))
            }
          producer.flush()
          duplicateSends.foreach(_.get(10, TimeUnit.SECONDS))

          val fillerSends = (0 until 16).map { index =>
            producer.send(ProducerRecord(topic, 0, null, randomBytes(1024, codecIndex * 1000L + index + 500L)))
          }
          producer.flush()
          fillerSends.foreach(_.get(10, TimeUnit.SECONDS))
        finally producer.close(Duration.ofSeconds(5))

        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos
        while broker.lifecycleStatistics.compactedBatches <= compactedBefore && System.nanoTime() < deadline do
          Thread.sleep(10L)
        assert(
          broker.lifecycleStatistics.compactedBatches > compactedBefore,
          s"$compression batch was not compacted before the deadline"
        )

        val keyed = consumeRecords(broker.bootstrapServers, topic).filter(_.key.nonEmpty)
        assertEquals(keyed.map(_.offset), (8L until 16L).toVector)
        assertEquals(keyed.map(_.key.get), keys.map(_.toVector))
        assertEquals(keyed.map(_.value), latestValues.map(_.toVector))
      }
    finally
      broker.close()
      deleteTree(directory)
  }

  private def createTopic(bootstrap: String, topic: String): Unit =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    val admin = Admin.create(properties)
    try admin.createTopics(java.util.List.of(NewTopic(topic, 1, 1.toShort))).all().get(10, TimeUnit.SECONDS): Unit
    finally admin.close(Duration.ofSeconds(5))

  private def consumeAvailable(bootstrap: String, topic: String): Vector[String] =
    consumeRecords(bootstrap, topic).map(record => String(record.value.toArray, StandardCharsets.UTF_8))

  private def consumeRecords(bootstrap: String, topic: String): Vector[ConsumedRecord] =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap))
    try
      val partition = KafkaTopicPartition(topic, 0)
      consumer.assign(java.util.List.of(partition))
      consumer.seekToBeginning(java.util.List.of(partition))
      val values = Vector.newBuilder[ConsumedRecord]
      var idlePolls = 0
      while idlePolls < 4 do
        val records = consumer.poll(Duration.ofMillis(250))
        if records.isEmpty then idlePolls += 1
        else
          idlePolls = 0
          records.iterator().asScala.foreach { record =>
            values += ConsumedRecord(
              record.offset(),
              Option(record.key()).map(_.toVector),
              Option(record.value()).fold(Vector.empty[Byte])(_.toVector)
            )
          }
      values.result()
    finally consumer.close()

  private def randomBytes(size: Int, seed: Long): Array[Byte] =
    val bytes = new Array[Byte](size)
    java.util.Random(seed).nextBytes(bytes)
    bytes

  private def producerProperties(bootstrap: String): Properties =
    val values = Properties()
    values.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    values.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    values.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    values.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    values.put(ProducerConfig.ACKS_CONFIG, "all")
    values.put(ProducerConfig.RETRIES_CONFIG, "0")
    values.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "3000")
    values.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "1000")
    values

  private def consumerProperties(bootstrap: String): Properties =
    val values = Properties()
    values.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    values.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    values.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    values.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    values.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    values.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "5000")
    values

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

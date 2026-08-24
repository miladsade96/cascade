package cascade.e2e

import cascade.backup.{BackupCreator, BackupRestore}
import cascade.broker.{BrokerConfig, KafkaBroker, RecoveryMode}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class BackupRecoveryEndToEndSuite extends munit.FunSuite:
  test("restored offline backup preserves exact Kafka-visible records") {
    val root = Files.createTempDirectory("cascade-backup-e2e")
    val original = root.resolve("original")
    val backup = root.resolve("backup")
    val restored = root.resolve("restored")
    try
      val first = broker(original)
      try
        first.start()
        val admin = Admin.create(adminProperties(first.bootstrapServers))
        try admin.createTopics(java.util.List.of(NewTopic("backup-events", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
        finally admin.close(Duration.ofSeconds(5))

        val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(first.bootstrapServers))
        try
          (0 until 100).foreach { index =>
            val metadata = producer.send(
              ProducerRecord[Array[Byte], Array[Byte]](
                "backup-events",
                0,
                null,
                f"backup-$index%03d".getBytes(StandardCharsets.UTF_8)
              )
            ).get(10, TimeUnit.SECONDS)
            assertEquals(metadata.offset(), index.toLong)
          }
        finally producer.close(Duration.ofSeconds(5))
      finally first.close()

      val manifest = BackupCreator.create(original, backup)
      val verified = BackupRestore.restore(backup, restored)
      assertEquals(verified.manifest, manifest)

      val recovered = broker(restored)
      try
        assertEquals(recovered.recoveryMode, RecoveryMode.Clean)
        recovered.start()
        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(recovered.bootstrapServers))
        try
          val partition = TopicPartition("backup-events", 0)
          consumer.assign(java.util.List.of(partition))
          consumer.seekToBeginning(java.util.List.of(partition))
          val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos
          val values = scala.collection.mutable.ArrayBuffer.empty[String]
          while values.size < 100 && System.nanoTime() < deadline do
            consumer.poll(Duration.ofMillis(200)).iterator().asScala.foreach { record =>
              values += String(record.value(), StandardCharsets.UTF_8)
            }
          assertEquals(values.toVector, (0 until 100).map(index => f"backup-$index%03d").toVector)
        finally consumer.close()
      finally recovered.close()
    finally deleteTree(root)
  }

  private def broker(directory: java.nio.file.Path): KafkaBroker =
    KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory
      )
    )

  private def adminProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties

  private def producerProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    properties

  private def consumerProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "backup-verifier")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

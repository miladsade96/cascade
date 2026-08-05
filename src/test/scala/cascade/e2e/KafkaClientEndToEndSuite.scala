package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class KafkaClientEndToEndSuite extends FunSuite:
  test("Apache Kafka producer and consumer interoperate with Cascade") {
    val directory = Files.createTempDirectory("cascade-e2e")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory
      )
    )
    try
      broker.start()
      val admin = Admin.create(adminProperties(broker.bootstrapServers))
      try admin.createTopics(java.util.List.of(new NewTopic("interop", 1, 1.toShort))).all().get()
      finally admin.close(Duration.ofSeconds(5))

      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(broker.bootstrapServers))
      try
        val metadata = producer.send(
          new ProducerRecord[Array[Byte], Array[Byte]](
            "interop",
            "language-neutral".getBytes(StandardCharsets.UTF_8)
          )
        ).get()
        assertEquals(metadata.offset(), 0L)
      finally producer.close(Duration.ofSeconds(5))

      val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(broker.bootstrapServers))
      try
        val partition = new TopicPartition("interop", 0)
        consumer.assign(java.util.List.of(partition))
        consumer.seekToBeginning(java.util.List.of(partition))
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos
        var value: Option[String] = None
        while value.isEmpty && System.nanoTime() < deadline do
          value = consumer.poll(Duration.ofMillis(250)).iterator().asScala
            .map(record => String(record.value(), StandardCharsets.UTF_8))
            .find(_ == "language-neutral")
        assertEquals(value, Some("language-neutral"))
      finally consumer.close()
    finally
      broker.close()
      deleteTree(directory)
  }

  private def producerProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000")
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties

  private def adminProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties

  private def consumerProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

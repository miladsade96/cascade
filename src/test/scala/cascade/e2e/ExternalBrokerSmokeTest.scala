package cascade.e2e

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Collections, Properties, UUID}
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

object ExternalBrokerSmokeTest:
  private val RecordCount = 25

  def main(arguments: Array[String]): Unit =
    require(
      arguments.length >= 1 && arguments.length <= 3 && arguments.drop(2).forall(_ == "--verify-only"),
      "usage: ExternalBrokerSmokeTest <bootstrap-host:port> [topic] [--verify-only]"
    )
    val bootstrapServers = arguments.head
    val topic = arguments.lift(1).getOrElse(s"container-smoke-${UUID.randomUUID()}")
    val verifyOnly = arguments.contains("--verify-only")
    verify(bootstrapServers, topic, verifyOnly)

  def verify(bootstrapServers: String, topic: String, verifyOnly: Boolean, security: Properties = Properties()): Unit =
    val clusterId = describeCluster(bootstrapServers, security)
    if !verifyOnly then produce(bootstrapServers, topic, security)
    consumeAndVerify(bootstrapServers, topic, security)
    val mode = if verifyOnly then "recovered" else "produced"
    println(s"Cascade container smoke test passed: cluster=$clusterId topic=$topic records=$RecordCount mode=$mode")

  private def describeCluster(bootstrapServers: String, security: Properties): String =
    val properties = security.clone().asInstanceOf[Properties]
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000")
    val admin = AdminClient.create(properties)
    try
      val description = admin.describeCluster()
      val nodes = description.nodes().get(20L, TimeUnit.SECONDS)
      require(!nodes.isEmpty, "broker returned no cluster nodes")
      description.clusterId().get(20L, TimeUnit.SECONDS)
    finally admin.close(Duration.ofSeconds(5L))

  private def produce(bootstrapServers: String, topic: String, security: Properties): Unit =
    val properties = security.clone().asInstanceOf[Properties]
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000")
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
    val producer = KafkaProducer[Array[Byte], Array[Byte]](properties)
    try
      (0 until RecordCount).foreach { index =>
        val key = s"key-$index".getBytes(StandardCharsets.UTF_8)
        val value = s"value-$index".getBytes(StandardCharsets.UTF_8)
        val metadata = producer.send(ProducerRecord(topic, 0, key, value)).get(30L, TimeUnit.SECONDS)
        require(metadata.offset() == index.toLong, s"unexpected offset ${metadata.offset()} for record $index")
      }
      producer.flush()
    finally producer.close(Duration.ofSeconds(5L))

  private def consumeAndVerify(bootstrapServers: String, topic: String, security: Properties): Unit =
    val properties = security.clone().asInstanceOf[Properties]
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, s"container-smoke-${UUID.randomUUID()}")
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](properties)
    val partition = TopicPartition(topic, 0)
    val observed = mutable.Map.empty[String, String]
    try
      consumer.assign(Collections.singleton(partition))
      consumer.seekToBeginning(Collections.singleton(partition))
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30L)
      while observed.size < RecordCount && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(250L)).records(partition).asScala.foreach { record =>
          observed.put(
            String(record.key(), StandardCharsets.UTF_8),
            String(record.value(), StandardCharsets.UTF_8)
          ): Unit
        }
      val expected = (0 until RecordCount).map(index => s"key-$index" -> s"value-$index").toMap
      require(observed.toMap == expected, s"expected $RecordCount exact records but received ${observed.size}")
    finally consumer.close()

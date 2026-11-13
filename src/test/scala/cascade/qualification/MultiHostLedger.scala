package cascade.qualification

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*
import scala.util.Try

/** External Kafka-client probe for three independent Docker-engine failure domains. */
object MultiHostLedger:
  private val TimeoutSeconds = 30L

  def main(arguments: Array[String]): Unit =
    require(arguments.nonEmpty, "a multi-host ledger action is required")
    arguments.head match
      case "produce" =>
        require(arguments.length == 6, "usage: MultiHostLedger produce <bootstrap> <topic> <start> <count> <create>")
        val start = arguments(3).toInt
        val count = arguments(4).toInt
        val create = arguments(5).toBoolean
        validate(arguments(1), arguments(2), start, count)
        produce(arguments(1), arguments(2), start, count, create)
      case "await-isr" =>
        require(arguments.length == 4, "usage: MultiHostLedger await-isr <bootstrap> <topic> <replicas>")
        awaitIsr(arguments(1), arguments(2), arguments(3).toInt)
      case "controller" =>
        require(arguments.length == 2, "usage: MultiHostLedger controller <bootstrap>")
        controller(arguments(1))
      case "expect-unavailable" =>
        require(arguments.length == 3, "usage: MultiHostLedger expect-unavailable <bootstrap> <topic>")
        expectUnavailable(arguments(1), arguments(2))
      case "verify" =>
        require(arguments.length == 4, "usage: MultiHostLedger verify <bootstrap> <topic> <count>")
        val count = arguments(3).toInt
        validate(arguments(1), arguments(2), 0, count)
        verify(arguments(1), arguments(2), count)
      case action => throw IllegalArgumentException(s"unknown multi-host ledger action: $action")

  private def produce(bootstrap: String, topic: String, start: Int, count: Int, create: Boolean): Unit =
    if create then
      val admin = Admin.create(adminProperties(bootstrap))
      try admin.createTopics(Collections.singleton(NewTopic(topic, 1, 3.toShort))).all().get(TimeoutSeconds, TimeUnit.SECONDS): Unit
      finally admin.close(Duration.ofSeconds(5))

    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap))
    try
      (start until start + count).foreach { index =>
        val metadata = producer.send(ProducerRecord(topic, 0, key(index), value(index))).get(TimeoutSeconds, TimeUnit.SECONDS)
        require(metadata.offset() == index.toLong, s"record $index received offset ${metadata.offset()}")
      }
      producer.flush()
    finally producer.close(Duration.ofSeconds(5))
    println(s"CASCADE_MULTI_HOST_RESULT {\"action\":\"produce\",\"start\":$start,\"count\":$count,\"last_offset\":${start + count - 1L}}")

  private def awaitIsr(bootstrap: String, topic: String, replicas: Int): Unit =
    require(replicas >= 1 && replicas <= 3, "expected ISR must be between one and three")
    val admin = Admin.create(adminProperties(bootstrap))
    try
      val deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos
      var observed = 0
      while observed != replicas && System.nanoTime() < deadline do
        val description = admin.describeTopics(Collections.singleton(topic)).allTopicNames()
          .get(10, TimeUnit.SECONDS).get(topic)
        observed = description.partitions().get(0).isr().size()
        if observed != replicas then Thread.sleep(250)
      require(observed == replicas, s"expected ISR $replicas but observed $observed")
      println(s"CASCADE_MULTI_HOST_RESULT {\"action\":\"await-isr\",\"isr\":$observed}")
    finally admin.close(Duration.ofSeconds(5))

  private def controller(bootstrap: String): Unit =
    val admin = Admin.create(adminProperties(bootstrap))
    try
      val quorum = admin.describeMetadataQuorum().quorumInfo().get(TimeoutSeconds, TimeUnit.SECONDS)
      require(quorum.leaderId() >= 1 && quorum.leaderId() <= 3, s"invalid controller ID ${quorum.leaderId()}")
      println(s"CASCADE_MULTI_HOST_RESULT {\"action\":\"controller\",\"controller_id\":${quorum.leaderId()},\"epoch\":${quorum.leaderEpoch()}}")
    finally admin.close(Duration.ofSeconds(5))

  private def expectUnavailable(bootstrap: String, topic: String): Unit =
    val producer = KafkaProducer[Array[Byte], Array[Byte]](unavailableProducerProperties(bootstrap))
    val rejected = try
      val record = ProducerRecord[Array[Byte], Array[Byte]](
        topic,
        0,
        null,
        "partition-probe".getBytes(StandardCharsets.UTF_8)
      )
      Try(producer.send(record).get(20, TimeUnit.SECONDS)).isFailure
    finally producer.close(Duration.ofSeconds(1))
    require(rejected, "a write was acknowledged while two of three failure domains were unavailable")
    println("CASCADE_MULTI_HOST_RESULT {\"action\":\"expect-unavailable\",\"acknowledged\":false}")

  private def verify(bootstrap: String, topic: String, count: Int): Unit =
    val group = s"multi-host-verifier-$topic"
    val partition = TopicPartition(topic, 0)
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap, group))
    val digest = MessageDigest.getInstance("SHA-256")
    var next = 0
    var previousCommitted = -1L
    try
      previousCommitted = Option(
        consumer.committed(Collections.singleton(partition), Duration.ofSeconds(10)).get(partition)
      ).map(_.offset()).getOrElse(-1L)
      consumer.assign(Collections.singleton(partition))
      consumer.seekToBeginning(Collections.singleton(partition))
      val deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos
      while next < count && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(250)).records(partition).asScala.foreach { record =>
          require(next < count, s"unexpected record at offset ${record.offset()}")
          require(record.offset() == next.toLong, s"expected offset $next but received ${record.offset()}")
          require(java.util.Arrays.equals(record.key(), key(next)), s"key mismatch at offset $next")
          require(java.util.Arrays.equals(record.value(), value(next)), s"value mismatch at offset $next")
          updateDigest(digest, next.toLong, record.key(), record.value())
          next += 1
        }
      require(next == count, s"expected $count records but received $next")
      require(consumer.poll(Duration.ofSeconds(1)).records(partition).isEmpty, "records exist beyond the expected ledger")
      consumer.commitSync(Map(partition -> OffsetAndMetadata(count.toLong)).asJava, Duration.ofSeconds(10))
    finally consumer.close(Duration.ofSeconds(5))
    val checksum = hex(digest.digest())
    require(checksum == checksumFor(count), "multi-host ledger checksum mismatch")
    println(
      s"CASCADE_MULTI_HOST_RESULT {\"action\":\"verify\",\"records\":$count,\"last_offset\":${count - 1L}," +
        s"\"previous_committed_offset\":$previousCommitted,\"committed_offset\":$count,\"ledger_sha256\":\"$checksum\"}"
    )

  private def validate(bootstrap: String, topic: String, start: Int, count: Int): Unit =
    require(bootstrap.nonEmpty, "bootstrap address is required")
    require(topic.matches("[a-zA-Z0-9._-]{1,249}"), "topic is invalid")
    require(start >= 0, "start offset cannot be negative")
    require(count >= 1 && Math.addExact(start, count) <= 1_000_000, "ledger range is invalid")

  private[qualification] def checksumFor(count: Int): String =
    val digest = MessageDigest.getInstance("SHA-256")
    (0 until count).foreach(index => updateDigest(digest, index.toLong, key(index), value(index)))
    hex(digest.digest())

  private def updateDigest(digest: MessageDigest, offset: Long, key: Array[Byte], value: Array[Byte]): Unit =
    digest.update(ByteBuffer.allocate(java.lang.Long.BYTES).putLong(offset).array())
    digest.update(ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(key.length).array())
    digest.update(key)
    digest.update(ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(value.length).array())
    digest.update(value)

  private def key(index: Int): Array[Byte] = f"mh-key-$index%08d".getBytes(StandardCharsets.UTF_8)
  private def value(index: Int): Array[Byte] = f"mh-value-$index%08d".getBytes(StandardCharsets.UTF_8)
  private def hex(bytes: Array[Byte]): String = bytes.map(value => f"${value & 0xff}%02x").mkString

  private def adminProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    properties

  private def producerProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000")
    properties

  private def unavailableProducerProperties(bootstrap: String): Properties =
    val properties = producerProperties(bootstrap)
    properties.setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    properties.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000")
    properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000")
    properties

  private def consumerProperties(bootstrap: String, group: String): Properties =
    val properties = Properties()
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group)
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    properties

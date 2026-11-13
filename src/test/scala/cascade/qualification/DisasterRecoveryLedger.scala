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

/** Kafka-bound exactness probe used by the external-object-store restore drill. */
object DisasterRecoveryLedger:
  private val Timeout = 30L

  def main(arguments: Array[String]): Unit =
    require(
      arguments.length == 5 && Set("produce", "verify").contains(arguments(0)),
      "usage: DisasterRecoveryLedger <produce|verify> <bootstrap> <topic> <group> <record-count>"
    )
    val mode = arguments(0)
    val bootstrap = arguments(1)
    val topic = arguments(2)
    val group = arguments(3)
    val count = arguments(4).toInt
    require(bootstrap.nonEmpty, "bootstrap address is required")
    require(topic.matches("[a-zA-Z0-9._-]{1,249}"), "topic is invalid")
    require(group.matches("[a-zA-Z0-9._-]{1,249}"), "group is invalid")
    require(count >= 1 && count <= 1_000_000, "record count must be between 1 and 1000000")

    mode match
      case "produce" => produceAndCommit(bootstrap, topic, group, count)
      case "verify"  => verifyRecovered(bootstrap, topic, group, count)

  private def produceAndCommit(bootstrap: String, topic: String, group: String, count: Int): Unit =
    val admin = Admin.create(adminProperties(bootstrap))
    try
      admin.createTopics(Collections.singleton(NewTopic(topic, 1, 1.toShort))).all().get(Timeout, TimeUnit.SECONDS)
    finally admin.close(Duration.ofSeconds(5))

    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap))
    var lastOffset = -1L
    try
      (0 until count).foreach { index =>
        val metadata = producer.send(
          ProducerRecord(topic, 0, key(index), value(index))
        ).get(Timeout, TimeUnit.SECONDS)
        require(metadata.offset() == index.toLong, s"record $index received offset ${metadata.offset()}")
        lastOffset = metadata.offset()
      }
      producer.flush()
    finally producer.close(Duration.ofSeconds(5))

    val acknowledgedAt = System.currentTimeMillis()
    val observed = consumeExact(bootstrap, topic, group, count, commit = true)
    val checksum = checksumFor(count)
    require(observed == checksum, "the pre-backup read did not match the deterministic ledger")
    println(
      s"CASCADE_DR_RESULT {\"mode\":\"produce\",\"records\":$count,\"last_offset\":$lastOffset," +
        s"\"committed_offset\":$count,\"last_ack_epoch_ms\":$acknowledgedAt,\"ledger_sha256\":\"$checksum\"}"
    )

  private def verifyRecovered(bootstrap: String, topic: String, group: String, count: Int): Unit =
    val observed = consumeExact(bootstrap, topic, group, count, commit = false)
    val expected = checksumFor(count)
    require(observed == expected, "the restored ledger checksum does not match the acknowledged ledger")

    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap, group))
    val partition = TopicPartition(topic, 0)
    try
      val committed = consumer.committed(Collections.singleton(partition), Duration.ofSeconds(10)).get(partition)
      require(committed != null, "the restored consumer-group offset is missing")
      require(committed.offset() == count.toLong, s"restored committed offset ${committed.offset()} != $count")
    finally consumer.close(Duration.ofSeconds(5))

    println(
      s"CASCADE_DR_RESULT {\"mode\":\"verify\",\"records\":$count,\"last_offset\":${count - 1L}," +
        s"\"committed_offset\":$count,\"ledger_sha256\":\"$observed\"}"
    )

  private def consumeExact(
      bootstrap: String,
      topic: String,
      group: String,
      count: Int,
      commit: Boolean
  ): String =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap, group))
    val partition = TopicPartition(topic, 0)
    val digest = MessageDigest.getInstance("SHA-256")
    var next = 0
    try
      consumer.assign(Collections.singleton(partition))
      consumer.seekToBeginning(Collections.singleton(partition))
      val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos
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
      val unexpected = consumer.poll(Duration.ofSeconds(1)).records(partition)
      require(unexpected.isEmpty, s"received ${unexpected.size()} records beyond the acknowledged ledger")
      if commit then consumer.commitSync(Map(partition -> OffsetAndMetadata(count.toLong)).asJava, Duration.ofSeconds(10))
      hex(digest.digest())
    finally consumer.close(Duration.ofSeconds(5))

  private[qualification] def checksumFor(count: Int): String =
    require(count >= 0, "record count cannot be negative")
    val digest = MessageDigest.getInstance("SHA-256")
    (0 until count).foreach(index => updateDigest(digest, index.toLong, key(index), value(index)))
    hex(digest.digest())

  private def updateDigest(digest: MessageDigest, offset: Long, key: Array[Byte], value: Array[Byte]): Unit =
    digest.update(ByteBuffer.allocate(java.lang.Long.BYTES).putLong(offset).array())
    digest.update(ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(key.length).array())
    digest.update(key)
    digest.update(ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(value.length).array())
    digest.update(value)

  private def key(index: Int): Array[Byte] = f"dr-key-$index%08d".getBytes(StandardCharsets.UTF_8)
  private def value(index: Int): Array[Byte] = f"dr-value-$index%08d".getBytes(StandardCharsets.UTF_8)
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

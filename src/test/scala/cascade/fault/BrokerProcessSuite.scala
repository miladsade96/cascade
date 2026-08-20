package cascade.fault

import cascade.broker.{BrokerConfig, KafkaBroker, RecoveryMode}
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class BrokerProcessSuite extends FunSuite:
  test("a forked broker can be force-killed without a cooperative shutdown") {
    val directory = Files.createTempDirectory("cascade-force-kill")
    val port = freePort()
    val broker = BrokerProcess.start(
      Seq(
        "--host", "127.0.0.1",
        "--port", port.toString,
        "--advertised-host", "127.0.0.1",
        "--advertised-port", port.toString,
        "--data-dir", directory.toString
      )
    )
    try
      broker.awaitListening("127.0.0.1", port)
      assert(broker.isAlive)
      broker.kill()
      assert(!broker.isAlive)
    finally
      broker.close()
      deleteTree(directory)
  }

  test("a force-killed broker enters unclean recovery on restart") {
    val directory = Files.createTempDirectory("cascade-unclean-restart")
    val port = freePort()
    val process = BrokerProcess.start(
      Seq(
        "--host", "127.0.0.1",
        "--port", port.toString,
        "--advertised-host", "127.0.0.1",
        "--advertised-port", port.toString,
        "--data-dir", directory.toString,
        "--flush-policy", "sync"
      )
    )
    try
      process.awaitListening("127.0.0.1", port)
      process.kill()
      val restarted = KafkaBroker(BrokerConfig(bindHost = "127.0.0.1", port = 0, dataDirectory = directory))
      try
        assertEquals(restarted.recoveryMode, RecoveryMode.Unclean)
        restarted.start()
      finally restarted.close()
    finally
      process.close()
      deleteTree(directory)
  }

  test("sync-flushed records recover exactly after a broker JVM is force-killed") {
    val directory = Files.createTempDirectory("cascade-kill-record-recovery")
    val port = freePort()
    val arguments = Seq(
      "--host", "127.0.0.1",
      "--port", port.toString,
      "--advertised-host", "127.0.0.1",
      "--advertised-port", port.toString,
      "--data-dir", directory.toString,
      "--flush-policy", "sync"
    )
    val bootstrap = s"127.0.0.1:$port"
    var process = BrokerProcess.start(arguments)
    try
      process.awaitListening("127.0.0.1", port)
      assertEquals(produce(bootstrap, "kill-events", "before-kill"), 0L)
      process.kill()

      process = BrokerProcess.start(arguments)
      process.awaitListening("127.0.0.1", port)
      val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap))
      try
        val partition = TopicPartition("kill-events", 0)
        consumer.assign(java.util.List.of(partition))
        consumer.seekToBeginning(java.util.List.of(partition))
        var values = Vector.empty[String]
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos
        while values.isEmpty && System.nanoTime() < deadline do
          consumer.poll(Duration.ofMillis(200)).iterator().asScala.foreach { record =>
            values :+= String(record.value(), StandardCharsets.UTF_8)
          }
        assertEquals(values, Vector("before-kill"))
      finally consumer.close()
      assertEquals(produce(bootstrap, "kill-events", "after-kill"), 1L)
    finally
      process.close()
      deleteTree(directory)
  }

  test("transaction outcomes and committed offsets recover after a coordinator JVM is force-killed") {
    val directory = Files.createTempDirectory("cascade-kill-coordinator-recovery")
    val port = freePort()
    val arguments = Seq(
      "--host", "127.0.0.1",
      "--port", port.toString,
      "--advertised-host", "127.0.0.1",
      "--advertised-port", port.toString,
      "--data-dir", directory.toString,
      "--flush-policy", "sync"
    )
    val bootstrap = s"127.0.0.1:$port"
    val topic = "kill-transaction-events"
    val partition = TopicPartition(topic, 0)
    val groupId = "kill-workers"
    var process = BrokerProcess.start(arguments)
    try
      process.awaitListening("127.0.0.1", port)
      val producer = KafkaProducer[Array[Byte], Array[Byte]](
        transactionalProducerProperties(bootstrap, "kill-transactional-producer")
      )
      try
        producer.initTransactions()
        producer.beginTransaction()
        producer.send(ProducerRecord(topic, "committed-before-kill".getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS)
        producer.commitTransaction()
      finally producer.close(Duration.ofSeconds(5))

      val offsetWriter = KafkaConsumer[Array[Byte], Array[Byte]](groupConsumerProperties(bootstrap, groupId))
      try
        offsetWriter.assign(java.util.List.of(partition))
        offsetWriter.commitSync(Map(partition -> OffsetAndMetadata(1L)).asJava)
      finally offsetWriter.close()
      process.kill()

      process = BrokerProcess.start(arguments)
      process.awaitListening("127.0.0.1", port)
      val committedReader = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap, readCommitted = true))
      try
        committedReader.assign(java.util.List.of(partition))
        committedReader.seekToBeginning(java.util.List.of(partition))
        var values = Vector.empty[String]
        val deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos
        while values.isEmpty && System.nanoTime() < deadline do
          committedReader.poll(Duration.ofMillis(200)).iterator().asScala.foreach { record =>
            values :+= String(record.value(), StandardCharsets.UTF_8)
          }
        assertEquals(values, Vector("committed-before-kill"))
      finally committedReader.close()

      val offsetReader = KafkaConsumer[Array[Byte], Array[Byte]](groupConsumerProperties(bootstrap, groupId))
      try
        assertEquals(Option(offsetReader.committed(java.util.Set.of(partition)).get(partition)).map(_.offset()), Some(1L))
      finally offsetReader.close()

      val successor = KafkaProducer[Array[Byte], Array[Byte]](
        transactionalProducerProperties(bootstrap, "kill-transactional-producer")
      )
      try
        successor.initTransactions()
        successor.beginTransaction()
        val appended = successor.send(ProducerRecord(topic, "committed-after-kill".getBytes(StandardCharsets.UTF_8)))
          .get(10, TimeUnit.SECONDS)
        assertEquals(appended.offset(), 1L)
        successor.commitTransaction()
      finally successor.close(Duration.ofSeconds(5))
    finally
      process.close()
      deleteTree(directory)
  }

  private def produce(bootstrap: String, topic: String, value: String): Long =
    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap))
    try producer.send(ProducerRecord(topic, value.getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS).offset()
    finally producer.close(Duration.ofSeconds(5))

  private def producerProperties(bootstrap: String): Properties =
    val values = Properties()
    values.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    values.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    values.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    values.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    values.put(ProducerConfig.ACKS_CONFIG, "all")
    values.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    values

  private def transactionalProducerProperties(bootstrap: String, transactionalId: String): Properties =
    val values = producerProperties(bootstrap)
    values.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    values.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    values.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000")
    values

  private def consumerProperties(bootstrap: String, readCommitted: Boolean = false): Properties =
    val values = Properties()
    values.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    values.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    values.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    values.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    values.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    values.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    values.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, if readCommitted then "read_committed" else "read_uncommitted")
    values

  private def groupConsumerProperties(bootstrap: String, groupId: String): Properties =
    val values = consumerProperties(bootstrap)
    values.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    values.put("group.protocol", "classic")
    values

  private def freePort(): Int =
    val socket = ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

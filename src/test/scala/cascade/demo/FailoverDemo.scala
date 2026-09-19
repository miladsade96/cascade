package cascade.demo

import java.nio.ByteBuffer
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final case class FailoverDemoConfig(
    bootstrapServers: String = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094",
    composeProject: String = "cascade-failover-demo",
    topic: String = "cascade-failover-demo",
    records: Int = 100_000,
    output: Option[Path] = None
):
  require(records >= 2, "the failover demo needs at least two records")

object FailoverDemoConfig:
  def parse(arguments: Array[String]): FailoverDemoConfig =
    @annotation.tailrec
    def loop(remaining: List[String], config: FailoverDemoConfig): FailoverDemoConfig = remaining match
      case Nil => config
      case "--bootstrap" :: value :: tail => loop(tail, config.copy(bootstrapServers = value))
      case "--compose-project" :: value :: tail => loop(tail, config.copy(composeProject = value))
      case "--topic" :: value :: tail => loop(tail, config.copy(topic = value))
      case "--records" :: value :: tail => loop(tail, config.copy(records = value.toInt))
      case "--output" :: value :: tail => loop(tail, config.copy(output = Some(Path.of(value))))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete option: $option")
    loop(arguments.toList, FailoverDemoConfig())

final case class FailoverDemoResult(
    oldLeader: Int,
    newLeader: Int,
    produced: Int,
    consumed: Int,
    lost: Int,
    unexpectedDuplicates: Int,
    failoverMillis: Long,
    elapsedMillis: Long
):
  def json: String =
    s"""{"old_leader":$oldLeader,"new_leader":$newLeader,"produced":$produced,"consumed":$consumed,"lost":$lost,"unexpected_duplicates":$unexpectedDuplicates,"failover_ms":$failoverMillis,"elapsed_ms":$elapsedMillis}"""

/** Real-container demo used by scripts/run-failover-demo.ps1. */
object FailoverDemo:
  def main(arguments: Array[String]): Unit =
    val config = FailoverDemoConfig.parse(arguments)
    val result = run(config)
    config.output.foreach { path =>
      Option(path.getParent).foreach(parent => Files.createDirectories(parent): Unit)
      Files.writeString(path, result.json + System.lineSeparator())
    }
    println(s"FAILOVER_RESULT ${result.json}")

  def run(config: FailoverDemoConfig): FailoverDemoResult =
    val started = System.nanoTime()
    val admin = Admin.create(adminProperties(config.bootstrapServers))
    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(config.bootstrapServers))
    try
      admin.createTopics(java.util.List.of(NewTopic(config.topic, 1, 3.toShort))).all().get(30, TimeUnit.SECONDS)
      val oldLeader = awaitLeader(admin, config.topic, excluded = None)
      awaitIsr(admin, config.topic, Set(1, 2, 3))
      val firstHalf = config.records / 2
      val acknowledged = AtomicLong(0L)
      val firstFailure = AtomicReference[Throwable]()
      sendRange(producer, config.topic, 0, firstHalf, acknowledged, firstFailure, None)
      producer.flush()
      checkFailure(firstFailure)
      if acknowledged.get() != firstHalf then
        throw IllegalStateException(s"expected $firstHalf pre-failover acknowledgements, got ${acknowledged.get()}")

      println(s"Current leader: broker-$oldLeader")
      println(s"Acknowledged before failure: $firstHalf")
      val failedAt = System.nanoTime()
      killContainer(config.composeProject, oldLeader)
      val firstPostFailoverAck = AtomicLong(0L)
      sendRange(producer, config.topic, firstHalf, config.records, acknowledged, firstFailure, Some(firstPostFailoverAck))
      producer.flush()
      checkFailure(firstFailure)
      val newLeader = awaitLeader(admin, config.topic, excluded = Some(oldLeader))
      val firstAck = firstPostFailoverAck.get()
      if firstAck == 0L then throw IllegalStateException("no record was acknowledged after leader failure")
      val failoverMillis = TimeUnit.NANOSECONDS.toMillis(firstAck - failedAt)
      if acknowledged.get() != config.records then
        throw IllegalStateException(s"expected ${config.records} acknowledgements, got ${acknowledged.get()}")

      println(s"Killed leader: broker-$oldLeader")
      println(s"New leader: broker-$newLeader")
      println(s"Acknowledged after recovery: ${config.records}")
      val (consumed, duplicates) = consumeAndVerify(config.bootstrapServers, config.topic, config.records)
      val lost = config.records - consumed
      if lost != 0 || duplicates != 0 then
        throw IllegalStateException(s"verification failed: lost=$lost unexpected_duplicates=$duplicates")
      FailoverDemoResult(
        oldLeader,
        newLeader,
        config.records,
        consumed,
        lost,
        duplicates,
        failoverMillis,
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
      )
    finally
      producer.close(Duration.ofSeconds(30))
      admin.close(Duration.ofSeconds(5))

  private def sendRange(
      producer: KafkaProducer[Array[Byte], Array[Byte]],
      topic: String,
      start: Int,
      end: Int,
      acknowledged: AtomicLong,
      firstFailure: AtomicReference[Throwable],
      firstPostFailoverAck: Option[AtomicLong]
  ): Unit =
    var index = start
    while index < end do
      val value = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(index.toLong).array()
      producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, 0, null, value), (_, error) =>
        if error == null then
          acknowledged.incrementAndGet(): Unit
          firstPostFailoverAck.foreach(_.compareAndSet(0L, System.nanoTime()): Unit)
        else firstFailure.compareAndSet(null, error): Unit
      )
      index += 1

  private def consumeAndVerify(bootstrap: String, topic: String, expected: Int): (Int, Int) =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap))
    val partition = TopicPartition(topic, 0)
    val seen = Array.fill(expected)(false)
    var unique = 0
    var duplicates = 0
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90)
    try
      consumer.assign(java.util.List.of(partition))
      consumer.seekToBeginning(java.util.List.of(partition))
      while unique < expected && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(250)).iterator().asScala.foreach { record =>
          if record.value().length != java.lang.Long.BYTES then
            throw IllegalStateException(s"unexpected payload size ${record.value().length} at offset ${record.offset()}")
          val index = ByteBuffer.wrap(record.value()).getLong()
          if index < 0L || index >= expected then throw IllegalStateException(s"unexpected record index $index")
          if seen(index.toInt) then duplicates += 1
          else
            seen(index.toInt) = true
            unique += 1
        }
      (unique, duplicates)
    finally consumer.close()

  private def awaitLeader(admin: Admin, topic: String, excluded: Option[Int]): Int =
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    var last = -1
    while System.nanoTime() < deadline do
      try
        val description = admin.describeTopics(java.util.List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS).get(topic)
        last = description.partitions().get(0).leader().id()
        if last >= 0 && !excluded.contains(last) then return last
      catch case _: Throwable => ()
      Thread.sleep(100L)
    throw IllegalStateException(s"timed out waiting for a new leader; last leader was $last")

  private def awaitIsr(admin: Admin, topic: String, expected: Set[Int]): Unit =
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
    while System.nanoTime() < deadline do
      val description = admin.describeTopics(java.util.List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS).get(topic)
      val replicas = description.partitions().get(0).isr().asScala.iterator.map(_.id()).toSet
      if replicas == expected then return
      Thread.sleep(100L)
    throw IllegalStateException(s"timed out waiting for ISR ${expected.toVector.sorted.mkString(",")}")

  private def killContainer(project: String, leaderId: Int): Unit =
    val name = s"$project-cascade-$leaderId-1"
    runCommand("docker", "update", "--restart=no", name)
    runCommand("docker", "kill", name)

  private def runCommand(command: String*): Unit =
    val process = ProcessBuilder(command*).inheritIO().start()
    if !process.waitFor(30, TimeUnit.SECONDS) then
      process.destroyForcibly(): Unit
      throw IllegalStateException(s"command timed out: ${command.mkString(" ")}")
    if process.exitValue() != 0 then throw IllegalStateException(s"command failed: ${command.mkString(" ")}")

  private def checkFailure(firstFailure: AtomicReference[Throwable]): Unit =
    Option(firstFailure.get()).foreach(throw _)

  private def adminProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    properties

  private def producerProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put(ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000")
    properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    properties.put(ProducerConfig.LINGER_MS_CONFIG, "5")
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, (128 * 1024).toString)
    properties

  private def consumerProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
    properties

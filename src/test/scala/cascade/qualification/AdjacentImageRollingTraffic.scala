package cascade.qualification

import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

private[qualification] final class AdjacentImageRollingTraffic private (
    bootstrapServers: String,
    topic: String,
    ledger: RollingTrafficLedger
) extends AutoCloseable:
  private val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
  private val executor = Executors.newVirtualThreadPerTaskExecutor()

  def leaderId: Int =
    val admin = Admin.create(properties(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "10000",
      AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG -> "30000"
    ))
    try
      admin.describeTopics(Collections.singleton(topic)).allTopicNames().get(30L, TimeUnit.SECONDS)
        .get(topic).partitions().getFirst.leader().id()
    finally admin.close(Duration.ofSeconds(5))

  def produce(label: String, count: Int): Unit =
    val planned = ledger.reserve(label, count)
    val offsets = send(planned, None)
    ledger.assertAcknowledged(planned, offsets)
    commitProgress()

  def produceDuring(label: String, count: Int)(transition: => Unit): Unit =
    val planned = ledger.reserve(label, count)
    val submitted = CountDownLatch(1)
    val task = executor.submit(new Callable[Vector[Long]]:
      override def call(): Vector[Long] = send(planned, Some(submitted))
    )
    require(submitted.await(30L, TimeUnit.SECONDS), "rolling transition traffic did not start")
    transition
    val offsets = task.get(4L, TimeUnit.MINUTES)
    ledger.assertAcknowledged(planned, offsets)
    commitProgress()

  def retryCount: Long =
    producer.metrics().asScala.iterator.collect {
      case (name, metric) if name.name() == "record-retry-total" =>
        metric.metricValue() match
          case value: java.lang.Number => value.longValue()
          case _ => 0L
    }.sum

  def awaitRetry(timeout: Duration): Unit =
    require(!timeout.isZero && !timeout.isNegative, "retry wait timeout must be positive")
    val deadline = System.nanoTime() + timeout.toNanos
    while retryCount == 0L && System.nanoTime() < deadline do Thread.sleep(25L)
    require(retryCount > 0L, "paused leader did not cause an idempotent producer retry")

  def awaitLeaderChange(previousLeader: Int, timeout: Duration): Int =
    require(!timeout.isZero && !timeout.isNegative, "leader change timeout must be positive")
    val deadline = System.nanoTime() + timeout.toNanos
    var currentLeader = previousLeader
    while currentLeader == previousLeader && System.nanoTime() < deadline do
      try currentLeader = leaderId
      catch case _: Exception => ()
      if currentLeader == previousLeader then Thread.sleep(100L)
    require(currentLeader != previousLeader, s"leader did not move away from paused node $previousLeader")
    currentLeader

  def verifyExact(): Unit =
    producer.flush()
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties)
    val partition = TopicPartition(topic, 0)
    val observed = ArrayBuffer.empty[(Long, String)]
    try
      consumer.assign(Collections.singleton(partition))
      consumer.seekToBeginning(Collections.singleton(partition))
      val deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos
      while observed.size < ledger.size && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(250)).records(partition).asScala.foreach { record =>
          observed += record.offset() -> String(record.value(), StandardCharsets.UTF_8)
        }
      ledger.assertObserved(observed.toVector)
      val committed = consumer.committed(Collections.singleton(partition)).get(partition)
      require(committed != null && committed.offset() == ledger.size.toLong,
        s"rolling committed offset mismatch: expected ${ledger.size}, got ${Option(committed).map(_.offset()).getOrElse(-1L)}")
    finally consumer.close()

  def recordCount: Int = ledger.size

  override def close(): Unit =
    executor.close()
    producer.close(Duration.ofSeconds(10))

  private def send(planned: Vector[(Long, String)], submitted: Option[CountDownLatch]): Vector[Long] =
    val futures = planned.zipWithIndex.map { case ((offset, value), index) =>
      val key = s"rolling-$offset".getBytes(StandardCharsets.UTF_8)
      val result = producer.send(ProducerRecord(topic, 0, key, value.getBytes(StandardCharsets.UTF_8)))
      if index == 0 then submitted.foreach(_.countDown())
      result
    }
    futures.map(_.get(180L, TimeUnit.SECONDS).offset())

  private def commitProgress(): Unit =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties)
    try consumer.commitSync(Map(TopicPartition(topic, 0) -> OffsetAndMetadata(ledger.size.toLong)).asJava)
    finally consumer.close()

  private def producerProperties: Properties = properties(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.ACKS_CONFIG -> "all",
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "true",
    ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION -> "5",
    ProducerConfig.RETRIES_CONFIG -> Int.MaxValue.toString,
    ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "10000",
    ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG -> "180000",
    "enable.metrics.push" -> "false"
  )

  private def consumerProperties: Properties = properties(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.GROUP_ID_CONFIG -> "adjacent-image-progress",
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG -> "10000",
    ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG -> "30000",
    "enable.metrics.push" -> "false"
  )

  private def properties(values: (String, String)*): Properties =
    val result = Properties()
    values.foreach { case (name, value) => result.put(name, value): Unit }
    result

private[qualification] object AdjacentImageRollingTraffic:
  def create(bootstrapServers: String, topic: String): AdjacentImageRollingTraffic =
    require(bootstrapServers.nonEmpty && topic.nonEmpty, "rolling traffic endpoint and topic must not be empty")
    val admin = Admin.create({
      val result = Properties()
      result.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
      result.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000")
      result.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "30000")
      result
    })
    try admin.createTopics(Collections.singleton(NewTopic(topic, 1, 3.toShort))).all().get(30L, TimeUnit.SECONDS): Unit
    finally admin.close(Duration.ofSeconds(5))
    AdjacentImageRollingTraffic(bootstrapServers, topic, RollingTrafficLedger())

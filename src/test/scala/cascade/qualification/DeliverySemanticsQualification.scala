package cascade.qualification

import cascade.broker.{BrokerConfig, KafkaBroker}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.*
import scala.jdk.CollectionConverters.*

final case class DeliveryQualificationConfig(
    transactions: Int = 256,
    concurrency: Int = 16,
    reportPath: Option[Path] = None
):
  require(transactions > 0 && transactions <= 10_000, "transactions must be between 1 and 10000")
  require(concurrency > 0 && concurrency <= 128, "concurrency must be between 1 and 128")
  require(concurrency <= transactions, "concurrency cannot exceed transactions")

object DeliveryQualificationConfig:
  def parse(arguments: Array[String]): DeliveryQualificationConfig =
    @annotation.tailrec
    def loop(values: List[String], config: DeliveryQualificationConfig): DeliveryQualificationConfig = values match
      case Nil => config
      case "--transactions" :: value :: tail => loop(tail, config.copy(transactions = value.toInt))
      case "--concurrency" :: value :: tail => loop(tail, config.copy(concurrency = value.toInt))
      case "--report" :: value :: tail => loop(tail, config.copy(reportPath = Some(Paths.get(value))))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete delivery qualification option: $option")
    loop(arguments.toList, DeliveryQualificationConfig())

final case class DeliveryQualificationReport(
    transactions: Int,
    concurrency: Int,
    committedRecords: Int,
    uncommittedRecords: Int,
    recoveredCommittedRecords: Int,
    mismatches: Int,
    elapsedSeconds: Double
):
  def passed: Boolean =
    committedRecords == transactions && uncommittedRecords == transactions * 2 &&
      recoveredCommittedRecords == transactions && mismatches == 0

  def json: String =
    f"""{"status":"${if passed then "passed" else "failed"}","transactions":$transactions,"concurrency":$concurrency,"committed_records":$committedRecords,"uncommitted_records":$uncommittedRecords,"recovered_committed_records":$recoveredCommittedRecords,"mismatches":$mismatches,"elapsed_seconds":$elapsedSeconds%.3f}"""

object DeliverySemanticsQualification:
  private val Topic = "delivery-qualification"

  def main(arguments: Array[String]): Unit =
    val report = run(DeliveryQualificationConfig.parse(arguments))
    println(s"DELIVERY_QUALIFICATION ${report.json}")
    if !report.passed then throw IllegalStateException("delivery qualification did not preserve exact visibility")

  def run(config: DeliveryQualificationConfig): DeliveryQualificationReport =
    val directory = Files.createTempDirectory("cascade-delivery-qualification")
    val brokerConfig = BrokerConfig(
      bindHost = "127.0.0.1",
      port = 0,
      advertisedHost = "127.0.0.1",
      dataDirectory = directory,
      autoCreateTopics = false
    )
    val started = System.nanoTime()
    var firstBroker: KafkaBroker = null
    var secondBroker: KafkaBroker = null
    try
      firstBroker = KafkaBroker(brokerConfig)
      firstBroker.start()
      createTopic(firstBroker.bootstrapServers)
      produceTransactions(firstBroker.bootstrapServers, config)
      val committed = consume(firstBroker.bootstrapServers, "read_committed", config.transactions)
      val uncommitted = consume(firstBroker.bootstrapServers, "read_uncommitted", config.transactions * 2)
      firstBroker.close()
      firstBroker = null

      secondBroker = KafkaBroker(brokerConfig)
      secondBroker.start()
      val recovered = consume(secondBroker.bootstrapServers, "read_committed", config.transactions)
      val expectedCommitted = (0 until config.transactions).map(index => s"commit-$index").toSet
      val expectedUncommitted = expectedCommitted ++ (0 until config.transactions).map(index => s"abort-$index")
      val mismatches = symmetricDifference(committed.toSet, expectedCommitted) +
        symmetricDifference(uncommitted.toSet, expectedUncommitted) +
        symmetricDifference(recovered.toSet, expectedCommitted)
      val report = DeliveryQualificationReport(
        config.transactions,
        config.concurrency,
        committed.size,
        uncommitted.size,
        recovered.size,
        mismatches,
        (System.nanoTime() - started) / 1_000_000_000d
      )
      config.reportPath.foreach { path =>
        Option(path.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
        Files.writeString(path, report.json + System.lineSeparator(), StandardCharsets.UTF_8): Unit
      }
      report
    finally
      if secondBroker != null then secondBroker.close()
      if firstBroker != null then firstBroker.close()
      deleteTree(directory)

  private def createTopic(bootstrap: String): Unit =
    val admin = Admin.create(properties(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap))
    try admin.createTopics(java.util.List.of(NewTopic(Topic, 1, 1.toShort))).all().get(30L, TimeUnit.SECONDS): Unit
    finally admin.close(Duration.ofSeconds(5))

  private def produceTransactions(bootstrap: String, config: DeliveryQualificationConfig): Unit =
    val executor = java.util.concurrent.Executors.newFixedThreadPool(config.concurrency)
    given ExecutionContext = ExecutionContext.fromExecutorService(executor)
    try
      val tasks = (0 until config.transactions).map { index =>
        Future {
          val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap, index))
          try
            producer.initTransactions()
            producer.beginTransaction()
            producer.send(ProducerRecord(Topic, 0, null, bytes(s"commit-$index"))).get(30L, TimeUnit.SECONDS)
            producer.commitTransaction()
            producer.beginTransaction()
            producer.send(ProducerRecord(Topic, 0, null, bytes(s"abort-$index"))).get(30L, TimeUnit.SECONDS)
            producer.abortTransaction()
          finally producer.close(Duration.ofSeconds(5))
        }
      }
      Await.result(Future.sequence(tasks), 10.minutes): Unit
    finally
      executor.shutdown()
      executor.awaitTermination(30L, TimeUnit.SECONDS): Unit

  private def consume(bootstrap: String, isolation: String, target: Int): Vector[String] =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap, isolation))
    val partition = TopicPartition(Topic, 0)
    try
      consumer.assign(java.util.List.of(partition))
      consumer.seekToBeginning(java.util.List.of(partition))
      val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos
      var values = Vector.empty[String]
      while values.size < target && System.nanoTime() < deadline do
        values ++= consumer.poll(Duration.ofMillis(100)).iterator().asScala.map(record => String(record.value(), StandardCharsets.UTF_8))
      if values.size != target then throw IllegalStateException(s"$isolation stopped at ${values.size}/$target records")
      values
    finally consumer.close()

  private def producerProperties(bootstrap: String, index: Int): Properties = properties(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.TRANSACTIONAL_ID_CONFIG -> s"delivery-qualification-$index",
    ProducerConfig.TRANSACTION_TIMEOUT_CONFIG -> "60000",
    ProducerConfig.ACKS_CONFIG -> "all",
    "enable.metrics.push" -> "false"
  )

  private def consumerProperties(bootstrap: String, isolation: String): Properties = properties(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
    ConsumerConfig.ISOLATION_LEVEL_CONFIG -> isolation,
    "enable.metrics.push" -> "false"
  )

  private def properties(values: (String, String)*): Properties =
    val result = Properties()
    values.foreach { case (name, value) => result.put(name, value): Unit }
    result

  private def bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

  private def symmetricDifference(actual: Set[String], expected: Set[String]): Int =
    (actual.diff(expected) ++ expected.diff(actual)).size

  private def deleteTree(root: Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

final class DeliverySemanticsQualificationSuite extends munit.FunSuite:
  test("transaction churn preserves committed visibility and restart recovery") {
    val report = DeliverySemanticsQualification.run(DeliveryQualificationConfig(transactions = 8, concurrency = 2))
    assert(report.passed, report.json)
    assertEquals(report.committedRecords, 8)
    assertEquals(report.uncommittedRecords, 16)
    assertEquals(report.recoveredCommittedRecords, 8)
  }

  test("delivery qualification arguments reject unsafe cardinality") {
    intercept[IllegalArgumentException](DeliveryQualificationConfig(transactions = 2, concurrency = 3))
    assertEquals(
      DeliveryQualificationConfig.parse(Array("--transactions", "32", "--concurrency", "4")),
      DeliveryQualificationConfig(transactions = 32, concurrency = 4)
    )
  }

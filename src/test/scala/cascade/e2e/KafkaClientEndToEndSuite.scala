package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.cluster.ClusterNode
import cascade.storage.FlushPolicy
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.net.ServerSocket
import java.time.Duration
import java.util.Collection
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
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

  test("subscribed Kafka consumer commits offsets across broker restart") {
    val directory = Files.createTempDirectory("cascade-group-e2e")
    try
      val firstBroker = testBroker(directory)
      try
        firstBroker.start()
        val admin = Admin.create(adminProperties(firstBroker.bootstrapServers))
        try admin.createTopics(java.util.List.of(new NewTopic("group-events", 2, 1.toShort))).all().get()
        finally admin.close(Duration.ofSeconds(5))

        val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(firstBroker.bootstrapServers))
        try
          (0 until 20).foreach { index =>
            producer.send(
              ProducerRecord[Array[Byte], Array[Byte]](
                "group-events",
                index % 2,
                null,
                s"value-$index".getBytes(StandardCharsets.UTF_8)
              )
            ).get()
          }
        finally producer.close(Duration.ofSeconds(5))

        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](groupConsumerProperties(firstBroker.bootstrapServers))
        try
          consumer.subscribe(java.util.List.of("group-events"))
          val values = pollValues(consumer, expected = 20)
          assertEquals(values.toSet, (0 until 20).map(index => s"value-$index").toSet)
          consumer.commitSync()
        finally consumer.close()
      finally firstBroker.close()

      val secondBroker = testBroker(directory)
      try
        secondBroker.start()
        val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(secondBroker.bootstrapServers))
        try
          producer.send(
            ProducerRecord[Array[Byte], Array[Byte]](
              "group-events",
              0,
              null,
              "after-restart".getBytes(StandardCharsets.UTF_8)
            )
          ).get()
        finally producer.close(Duration.ofSeconds(5))

        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](groupConsumerProperties(secondBroker.bootstrapServers))
        try
          consumer.subscribe(java.util.List.of("group-events"))
          assertEquals(pollValues(consumer, expected = 1), Vector("after-restart"))
        finally consumer.close()
      finally secondBroker.close()
    finally deleteTree(directory)
  }

  test("two subscribed consumers rebalance partitions without duplicate delivery") {
    val directory = Files.createTempDirectory("cascade-rebalance-e2e")
    val broker = testBroker(directory)
    try
      broker.start()
      val admin = Admin.create(adminProperties(broker.bootstrapServers))
      try admin.createTopics(java.util.List.of(new NewTopic("rebalance-events", 4, 1.toShort))).all().get()
      finally admin.close(Duration.ofSeconds(5))

      val seen = ConcurrentHashMap.newKeySet[String]()
      val duplicates = AtomicInteger(0)
      val perConsumer = Array.fill(2)(AtomicInteger(0))
      val assigned = Array.fill(2)(AtomicBoolean(false))
      val bothAssigned = CountDownLatch(2)
      val executor = Executors.newFixedThreadPool(2)
      try
        val tasks = (0 until 2).map { worker =>
          executor.submit(new Callable[Unit]:
            override def call(): Unit =
              val consumer = KafkaConsumer[Array[Byte], Array[Byte]](
                groupConsumerProperties(broker.bootstrapServers, "rebalance-workers", maxPollRecords = 1)
              )
              try
                consumer.subscribe(
                  java.util.List.of("rebalance-events"),
                  new ConsumerRebalanceListener:
                    override def onPartitionsRevoked(partitions: Collection[TopicPartition]): Unit = ()
                    override def onPartitionsAssigned(partitions: Collection[TopicPartition]): Unit =
                      if !partitions.isEmpty && assigned(worker).compareAndSet(false, true) then bothAssigned.countDown()
                )
                val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos
                while seen.size() < 400 && System.nanoTime() < deadline do
                  consumer.poll(Duration.ofMillis(100)).iterator().asScala.foreach { record =>
                    val value = String(record.value(), StandardCharsets.UTF_8)
                    if !seen.add(value) then duplicates.incrementAndGet(): Unit
                    perConsumer(worker).incrementAndGet(): Unit
                  }
              finally consumer.close()
          )
        }
        assert(bothAssigned.await(15, TimeUnit.SECONDS))
        val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(broker.bootstrapServers))
        try
          (0 until 400).foreach { index =>
            producer.send(
              ProducerRecord[Array[Byte], Array[Byte]](
                "rebalance-events",
                index % 4,
                null,
                s"rebalance-$index".getBytes(StandardCharsets.UTF_8)
              )
            ).get()
          }
        finally producer.close(Duration.ofSeconds(5))
        tasks.foreach(_.get(35, TimeUnit.SECONDS))
      finally
        executor.shutdownNow(): Unit
        executor.awaitTermination(5, TimeUnit.SECONDS): Unit

      assertEquals(seen.size(), 400)
      assertEquals(duplicates.get(), 0)
      assert(perConsumer.forall(_.get() > 0))
    finally
      broker.close()
      deleteTree(directory)
  }

  test("three brokers replicate records and promote a surviving partition leader") {
    val ports = freePorts(3)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "127.0.0.1", port) }
    val directories = nodes.map(node => Files.createTempDirectory(s"cascade-cluster-${node.id}"))
    val brokers = nodes.zip(directories).map { case (node, directory) =>
      KafkaBroker(
        BrokerConfig(
          bindHost = "127.0.0.1",
          port = node.port,
          advertisedHost = node.host,
          advertisedPort = Some(node.port),
          dataDirectory = directory,
          flushPolicy = FlushPolicy.Sync,
          nodeId = node.id,
          clusterNodes = nodes,
          controllerId = 1,
          defaultReplicationFactor = 3,
          minInSyncReplicas = 2,
          peerTimeoutMillis = 1000
        )
      )
    }
    val bootstrapServers = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    try
      brokers.foreach(_.start())
      val admin = Admin.create(adminProperties(bootstrapServers))
      try admin.createTopics(java.util.List.of(new NewTopic("replicated-events", 2, 3.toShort))).all().get()
      finally admin.close(Duration.ofSeconds(5))

      val firstProducer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrapServers))
      try
        val metadata = firstProducer.send(
          ProducerRecord[Array[Byte], Array[Byte]](
            "replicated-events",
            1,
            null,
            "before-failover".getBytes(StandardCharsets.UTF_8)
          )
        ).get(15, TimeUnit.SECONDS)
        assertEquals(metadata.offset(), 0L)
      finally firstProducer.close(Duration.ofSeconds(5))

      brokers(1).close()
      Thread.sleep(2500L)

      val secondProducer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrapServers))
      try
        val metadata = secondProducer.send(
          ProducerRecord[Array[Byte], Array[Byte]](
            "replicated-events",
            1,
            null,
            "after-failover".getBytes(StandardCharsets.UTF_8)
          )
        ).get(15, TimeUnit.SECONDS)
        assertEquals(metadata.offset(), 1L)
      finally secondProducer.close(Duration.ofSeconds(5))

      val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrapServers))
      try
        val partition = TopicPartition("replicated-events", 1)
        consumer.assign(java.util.List.of(partition))
        consumer.seekToBeginning(java.util.List.of(partition))
        assertEquals(pollValues(consumer, expected = 2), Vector("before-failover", "after-failover"))
      finally consumer.close()
    finally
      brokers.foreach(_.close())
      directories.foreach(deleteTree)
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

  private def groupConsumerProperties(
      bootstrapServers: String,
      groupId: String = "cascade-workers",
      maxPollRecords: Int = 500
  ): Properties =
    val properties = consumerProperties(bootstrapServers)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put("group.protocol", "classic")
    properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000")
    properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    properties

  private def pollValues(consumer: KafkaConsumer[Array[Byte], Array[Byte]], expected: Int): Vector[String] =
    val values = Vector.newBuilder[String]
    var count = 0
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    while count < expected && System.nanoTime() < deadline do
      val records = consumer.poll(Duration.ofMillis(250))
      records.iterator().asScala.foreach { record =>
        values += String(record.value(), StandardCharsets.UTF_8)
        count += 1
      }
    val result = values.result()
    assertEquals(result.size, expected)
    result

  private def testBroker(directory: java.nio.file.Path): KafkaBroker =
    KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory
      )
    )

  private def freePorts(count: Int): Vector[Int] =
    val sockets = Vector.fill(count)(ServerSocket(0))
    try sockets.map(_.getLocalPort)
    finally sockets.foreach(_.close())

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

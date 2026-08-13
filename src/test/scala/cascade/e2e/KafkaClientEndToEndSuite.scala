package cascade.e2e

import cascade.TestRecordBatch
import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.cluster.{ClusterNode, InternalApi, MetadataCodec, PeerClient}
import cascade.protocol.{ByteWriter, Errors}
import cascade.storage.{FlushPolicy, PartitionLog}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.net.ServerSocket
import java.time.Duration
import java.util.Collection
import java.util.Optional
import java.util.Properties
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{Callable, ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewPartitionReassignment, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerGroupMetadata, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{InvalidReplicaAssignmentException, NoReassignmentInProgressException}
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

  test("a quorum elects a new controller, fences the old term, and keeps serving Kafka clients") {
    val ports = freePorts(3)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "127.0.0.1", port) }
    val directories = nodes.map(node => Files.createTempDirectory(s"cascade-controller-${node.id}"))
    val configs = nodes.zip(directories).map { case (node, directory) =>
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
        peerTimeoutMillis = 800,
        controllerHeartbeatMillis = 100,
        controllerElectionTimeoutMillis = 600
      )
    }
    val brokers = configs.map(KafkaBroker(_))
    val bootstrapServers = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    var restartedBroker: Option[KafkaBroker] = None
    try
      brokers.foreach(_.start())
      val admin = Admin.create(adminProperties(bootstrapServers))
      try
        val firstController = awaitController(admin)
        val firstTerm = controllerTerm(nodes(firstController - 1))
        admin.createTopics(java.util.List.of(new NewTopic("controller-events", 3, 3.toShort)))
          .all().get(20, TimeUnit.SECONDS)
        val partition = firstController - 1
        awaitInSyncReplicas(admin, "controller-events", partition, Set(1, 2, 3))
        produceValue(bootstrapServers, "controller-events", partition, "before-controller-loss", expectedOffset = 0L)

        brokers(firstController - 1).close()
        val nextController = awaitController(admin, excludedId = Some(firstController))
        assertNotEquals(nextController, firstController)

        val peer = PeerClient()
        try
          val staleHeartbeat = peer.call(
            nodes(nextController - 1),
            InternalApi.ControllerHeartbeat,
            ByteWriter()
              .writeLong(firstTerm)
              .writeInt(firstController)
              .writeLong(firstTerm)
              .writeLong(0L)
              .result(),
            1000
          )
          assert(staleHeartbeat.readLong() > firstTerm)
          assertEquals(staleHeartbeat.readShort(), Errors.NotController)
          staleHeartbeat.readLong()
          staleHeartbeat.readLong()
          staleHeartbeat.ensureFullyRead()
        finally peer.close()

        admin.createTopics(java.util.List.of(new NewTopic("created-after-election", 1, 2.toShort)))
          .all().get(20, TimeUnit.SECONDS)
        val survivors = Set(1, 2, 3) - firstController
        awaitInSyncReplicas(admin, "controller-events", partition, survivors)
        produceValue(bootstrapServers, "controller-events", partition, "after-controller-loss", expectedOffset = 1L)

        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrapServers))
        try
          val topicPartition = TopicPartition("controller-events", partition)
          consumer.assign(java.util.List.of(topicPartition))
          consumer.seekToBeginning(java.util.List.of(topicPartition))
          assertEquals(
            pollValues(consumer, expected = 2),
            Vector("before-controller-loss", "after-controller-loss")
          )
        finally consumer.close()

        val replacement = KafkaBroker(configs(firstController - 1))
        restartedBroker = Some(replacement)
        replacement.start()
        awaitInSyncReplicas(admin, "controller-events", partition, Set(1, 2, 3))
        assertEquals(awaitController(admin), nextController)
      finally admin.close(Duration.ofSeconds(5))
    finally
      restartedBroker.foreach(_.close())
      brokers.foreach(_.close())
      directories.foreach(deleteTree)
  }

  test("an isolated controller loses its quorum lease and fences replica writes") {
    val ports = freePorts(3)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "127.0.0.1", port) }
    val directories = nodes.map(node => Files.createTempDirectory(s"cascade-controller-lease-${node.id}"))
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
          peerTimeoutMillis = 300,
          controllerHeartbeatMillis = 100,
          controllerElectionTimeoutMillis = 600
        )
      )
    }
    val bootstrapServers = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    try
      brokers.foreach(_.start())
      val admin = Admin.create(adminProperties(bootstrapServers))
      try
        val controllerId = awaitController(admin)
        admin.createTopics(java.util.List.of(new NewTopic("lease-events", 3, 3.toShort)))
          .all().get(20, TimeUnit.SECONDS)
        val partition = controllerId - 1
        awaitInSyncReplicas(admin, "lease-events", partition, Set(1, 2, 3))
        val metadata = clusterMetadata(nodes(controllerId - 1))
        val leaderEpoch = metadata.byName("lease-events").partitions(partition).leaderEpoch

        nodes.indices.filter(_ != controllerId - 1).foreach(index => brokers(index).close())
        val peer = PeerClient()
        try
          val deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos
          var error = Errors.None
          while error != Errors.BrokerNotAvailable && System.nanoTime() < deadline do
            val response = peer.call(
              nodes(controllerId - 1),
              InternalApi.ReplicaCommit,
              ByteWriter()
                .writeString("lease-events")
                .writeInt(partition)
                .writeInt(leaderEpoch)
                .writeLong(0L)
                .result(),
              1000
            )
            error = response.readShort()
            response.ensureFullyRead()
            if error != Errors.BrokerNotAvailable then Thread.sleep(100L)
          assertEquals(error, Errors.BrokerNotAvailable)
        finally peer.close()
      finally admin.close(Duration.ofSeconds(5))
    finally
      brokers.foreach(_.close())
      directories.foreach(deleteTree)
  }

  test("a returning replica replaces a divergent tail before it rejoins the ISR") {
    val ports = freePorts(3)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "127.0.0.1", port) }
    val directories = nodes.map(node => Files.createTempDirectory(s"cascade-rejoin-${node.id}"))
    val configs = nodes.zip(directories).map { case (node, directory) =>
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
    }
    val brokers = configs.map(KafkaBroker(_))
    val bootstrapServers = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    var returnedBroker: Option[KafkaBroker] = None
    try
      try
        brokers.foreach(_.start())
        val admin = Admin.create(adminProperties(bootstrapServers))
        try
          admin.createTopics(java.util.List.of(new NewTopic("recovering-events", 2, 3.toShort))).all().get()
          awaitInSyncReplicas(admin, "recovering-events", 1, Set(1, 2, 3))

          produceValue(bootstrapServers, "recovering-events", 1, "before-failure", expectedOffset = 0L)
          (1 to 32).foreach { offset =>
            produceValue(bootstrapServers, "recovering-events", 1, s"shared-$offset", expectedOffset = offset.toLong)
          }
          val sharedPrefixBytes = Files.size(
            directories(2).resolve("recovering-events").resolve("partition-1").resolve("00000000000000000000.log")
          )
          brokers(2).close()
          awaitInSyncReplicas(admin, "recovering-events", 1, Set(1, 2))
          produceValue(bootstrapServers, "recovering-events", 1, "while-away", expectedOffset = 33L)

          val divergent = PartitionLog(
            directories(2).resolve("recovering-events").resolve("partition-1"),
            flushPolicy = FlushPolicy.Sync
          )
          try
            assertEquals(divergent.logEndOffset, 33L)
            assertEquals(divergent.highWatermark, 33L)
            assertEquals(divergent.append(TestRecordBatch.single(totalBytes = 100)).baseOffset, 33L)
            assertEquals(divergent.logEndOffset, 34L)
          finally divergent.close()

          val replacement = KafkaBroker(configs(2))
          returnedBroker = Some(replacement)
          replacement.start()
          awaitInSyncReplicas(admin, "recovering-events", 1, Set(1, 2, 3))
          assertEquals(
            Files.size(
              directories(2).resolve("recovering-events").resolve("partition-1").resolve("00000000000000000000.log")
            ) > sharedPrefixBytes,
            true
          )
          produceValue(bootstrapServers, "recovering-events", 1, "after-recovery", expectedOffset = 34L)
        finally admin.close(Duration.ofSeconds(5))
      finally
        returnedBroker.foreach(_.close())
        brokers.foreach(_.close())

      val leader = PartitionLog(
        directories(1).resolve("recovering-events").resolve("partition-1"),
        flushPolicy = FlushPolicy.Sync
      )
      val recovered = PartitionLog(
        directories(2).resolve("recovering-events").resolve("partition-1"),
        flushPolicy = FlushPolicy.Sync
      )
      try
        assertEquals(leader.logEndOffset, 35L)
        assertEquals(recovered.logEndOffset, leader.logEndOffset)
        assert(
          recovered.fetch(0L, 1024 * 1024).records.sameElements(leader.fetch(0L, 1024 * 1024).records),
          "the recovered replica must exactly match the authoritative leader log"
        )
      finally
        recovered.close()
        leader.close()
    finally directories.foreach(deleteTree)
  }

  test("Kafka Admin lists and cancels a durable partition reassignment") {
    val ports = freePorts(3)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "127.0.0.1", port) }
    val directories = nodes.map(node => Files.createTempDirectory(s"cascade-reassignment-cancel-${node.id}"))
    val configs = nodes.zip(directories).map { case (node, directory) =>
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
        defaultReplicationFactor = 2,
        minInSyncReplicas = 2,
        peerTimeoutMillis = 500,
        controllerHeartbeatMillis = 100,
        controllerElectionTimeoutMillis = 600
      )
    }
    val brokers = configs.map(KafkaBroker(_))
    val bootstrapServers = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    try
      brokers.take(2).foreach(_.start())
      val admin = Admin.create(adminProperties(bootstrapServers))
      try
        awaitController(admin)
        admin.createTopics(java.util.List.of(new NewTopic("cancel-reassignment", 1, 2.toShort)))
          .all().get(20, TimeUnit.SECONDS)
        val topicPartition = TopicPartition("cancel-reassignment", 0)
        awaitReplicaMembership(admin, topicPartition, Set(1, 2), Set(1, 2))

        val invalid = intercept[java.util.concurrent.ExecutionException] {
          admin.alterPartitionReassignments(
            Map(
              topicPartition -> Optional.of(NewPartitionReassignment(java.util.List.of(Integer.valueOf(99))))
            ).asJava
          ).all().get(20, TimeUnit.SECONDS)
        }
        assert(invalid.getCause.isInstanceOf[InvalidReplicaAssignmentException])

        admin.alterPartitionReassignments(
          Map(
            topicPartition -> Optional.of(NewPartitionReassignment(java.util.List.of(Integer.valueOf(2), Integer.valueOf(3))))
          ).asJava
        ).all().get(20, TimeUnit.SECONDS)

        val ongoing = awaitReassignment(admin, topicPartition)
        assertEquals(ongoing.replicas().asScala.map(_.intValue()).toVector, Vector(2, 3, 1))
        assertEquals(ongoing.addingReplicas().asScala.map(_.intValue()).toVector, Vector(3))
        assertEquals(ongoing.removingReplicas().asScala.map(_.intValue()).toVector, Vector(1))

        admin.alterPartitionReassignments(
          Map(topicPartition -> Optional.empty[NewPartitionReassignment]()).asJava
        ).all().get(20, TimeUnit.SECONDS)
        awaitNoReassignment(admin, topicPartition)
        awaitReplicaMembership(admin, topicPartition, Set(1, 2), Set(1, 2))

        val missing = intercept[java.util.concurrent.ExecutionException] {
          admin.alterPartitionReassignments(
            Map(topicPartition -> Optional.empty[NewPartitionReassignment]()).asJava
          ).all().get(20, TimeUnit.SECONDS)
        }
        assert(missing.getCause.isInstanceOf[NoReassignmentInProgressException])

        brokers(2).start()
        awaitController(admin)
        awaitReplicaMembership(admin, topicPartition, Set(1, 2), Set(1, 2))
      finally admin.close(Duration.ofSeconds(5))
    finally
      brokers.foreach(_.close())
      directories.foreach(deleteTree)
  }

  test("one reassignment catches up multiple learners and transfers leadership") {
    val ports = freePorts(4)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "127.0.0.1", port) }
    val directories = nodes.map(node => Files.createTempDirectory(s"cascade-reassignment-learners-${node.id}"))
    val configs = nodes.zip(directories).map { case (node, directory) =>
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
        defaultReplicationFactor = 1,
        minInSyncReplicas = 1,
        peerTimeoutMillis = 800,
        controllerHeartbeatMillis = 100,
        controllerElectionTimeoutMillis = 800,
        replicaRecoveryChunkBytes = 4096
      )
    }
    val brokers = configs.map(KafkaBroker(_))
    val bootstrapServers = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    try
      brokers.foreach(_.start())
      val admin = Admin.create(adminProperties(bootstrapServers))
      try
        awaitController(admin)
        admin.createTopics(java.util.List.of(new NewTopic("multi-learner-events", 1, 1.toShort)))
          .all().get(20, TimeUnit.SECONDS)
        awaitInSyncReplicas(admin, "multi-learner-events", 0, Set(1))
        (0 until 32).foreach { index =>
          produceValue(bootstrapServers, "multi-learner-events", 0, s"learner-$index", expectedOffset = index.toLong)
        }

        val topicPartition = TopicPartition("multi-learner-events", 0)
        admin.alterPartitionReassignments(
          Map(
            topicPartition -> Optional.of(
              NewPartitionReassignment(
                java.util.List.of(Integer.valueOf(2), Integer.valueOf(3), Integer.valueOf(4))
              )
            )
          ).asJava
        ).all().get(20, TimeUnit.SECONDS)
        awaitNoReassignment(admin, topicPartition)
        awaitReplicaMembership(admin, topicPartition, Set(2, 3, 4), Set(2, 3, 4))

        val description = admin.describeTopics(java.util.List.of("multi-learner-events"))
          .allTopicNames().get(5, TimeUnit.SECONDS).get("multi-learner-events")
        assertEquals(description.partitions().get(0).leader().id(), 2)
        produceValue(bootstrapServers, "multi-learner-events", 0, "after-learners", expectedOffset = 32L)

        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrapServers))
        try
          consumer.assign(java.util.List.of(topicPartition))
          consumer.seekToBeginning(java.util.List.of(topicPartition))
          assertEquals(
            pollValues(consumer, expected = 33),
            (0 until 32).map(index => s"learner-$index").toVector :+ "after-learners"
          )
        finally consumer.close()
      finally admin.close(Duration.ofSeconds(5))
    finally
      brokers.foreach(_.close())
      directories.foreach(deleteTree)
  }

  test("reassignment survives controller loss and moves live data without loss") {
    val ports = freePorts(3)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "127.0.0.1", port) }
    val directories = nodes.map(node => Files.createTempDirectory(s"cascade-reassignment-failover-${node.id}"))
    val configs = nodes.zip(directories).map { case (node, directory) =>
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
        defaultReplicationFactor = 2,
        minInSyncReplicas = 2,
        peerTimeoutMillis = 500,
        controllerHeartbeatMillis = 100,
        controllerElectionTimeoutMillis = 600,
        replicaRecoveryChunkBytes = 4096
      )
    }
    val brokers = configs.map(KafkaBroker(_))
    val bootstrapServers = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    try
      brokers.take(2).foreach(_.start())
      val admin = Admin.create(adminProperties(bootstrapServers))
      try
        val firstController = awaitController(admin)
        assert(Set(1, 2).contains(firstController))
        admin.createTopics(java.util.List.of(new NewTopic("moving-events", 1, 2.toShort)))
          .all().get(20, TimeUnit.SECONDS)
        awaitInSyncReplicas(admin, "moving-events", 0, Set(1, 2))

        val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrapServers))
        try
          (0 until 64).foreach { index =>
            val metadata = producer.send(
              ProducerRecord[Array[Byte], Array[Byte]](
                "moving-events",
                0,
                null,
                s"before-move-$index".getBytes(StandardCharsets.UTF_8)
              )
            ).get(15, TimeUnit.SECONDS)
            assertEquals(metadata.offset(), index.toLong)
          }
        finally producer.close(Duration.ofSeconds(5))

        val topicPartition = TopicPartition("moving-events", 0)
        admin.alterPartitionReassignments(
          Map(
            topicPartition -> Optional.of(NewPartitionReassignment(java.util.List.of(Integer.valueOf(2), Integer.valueOf(3))))
          ).asJava
        ).all().get(20, TimeUnit.SECONDS)
        awaitReassignment(admin, topicPartition)

        brokers(firstController - 1).close()
        brokers(2).start()
        val nextController = awaitController(admin, excludedId = Some(firstController))
        assertNotEquals(nextController, firstController)
        awaitNoReassignment(admin, topicPartition)
        awaitReplicaMembership(admin, topicPartition, Set(2, 3), Set(2, 3))

        produceValue(bootstrapServers, "moving-events", 0, "after-move", expectedOffset = 64L)
        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrapServers))
        try
          consumer.assign(java.util.List.of(topicPartition))
          consumer.seekToBeginning(java.util.List.of(topicPartition))
          assertEquals(
            pollValues(consumer, expected = 65),
            (0 until 64).map(index => s"before-move-$index").toVector :+ "after-move"
          )
        finally consumer.close()
      finally admin.close(Duration.ofSeconds(5))
    finally
      brokers.foreach(_.close())
      directories.foreach(deleteTree)
  }

  test("Kafka transactions expose committed records, hide aborted records, and recover after restart") {
    val directory = Files.createTempDirectory("cascade-transactions-e2e")
    try
      val firstBroker = testBroker(directory)
      try
        firstBroker.start()
        val admin = Admin.create(adminProperties(firstBroker.bootstrapServers))
        try admin.createTopics(java.util.List.of(new NewTopic("transaction-events", 1, 1.toShort))).all().get()
        finally admin.close(Duration.ofSeconds(5))

        val producer = KafkaProducer[Array[Byte], Array[Byte]](
          transactionalProducerProperties(firstBroker.bootstrapServers, "recoverable-producer")
        )
        try
          producer.initTransactions()
          producer.beginTransaction()
          producer.send(ProducerRecord("transaction-events", "committed-before-restart".getBytes(StandardCharsets.UTF_8))).get()
          producer.commitTransaction()

          producer.beginTransaction()
          producer.send(ProducerRecord("transaction-events", "aborted-before-restart".getBytes(StandardCharsets.UTF_8))).get()
          producer.abortTransaction()
        finally producer.close(Duration.ofSeconds(5))
      finally firstBroker.close()

      val secondBroker = testBroker(directory)
      try
        secondBroker.start()
        val producer = KafkaProducer[Array[Byte], Array[Byte]](
          transactionalProducerProperties(secondBroker.bootstrapServers, "recoverable-producer")
        )
        try
          producer.initTransactions()
          producer.beginTransaction()
          producer.send(ProducerRecord("transaction-events", "committed-after-restart".getBytes(StandardCharsets.UTF_8))).get()
          producer.commitTransaction()
        finally producer.close(Duration.ofSeconds(5))

        val committedConsumer = KafkaConsumer[Array[Byte], Array[Byte]](
          consumerProperties(secondBroker.bootstrapServers, readCommitted = true)
        )
        try
          val partition = TopicPartition("transaction-events", 0)
          committedConsumer.assign(java.util.List.of(partition))
          committedConsumer.seekToBeginning(java.util.List.of(partition))
          assertEquals(
            pollValues(committedConsumer, expected = 2),
            Vector("committed-before-restart", "committed-after-restart")
          )
        finally committedConsumer.close()

        val uncommittedConsumer = KafkaConsumer[Array[Byte], Array[Byte]](
          consumerProperties(secondBroker.bootstrapServers, readCommitted = false)
        )
        try
          val partition = TopicPartition("transaction-events", 0)
          uncommittedConsumer.assign(java.util.List.of(partition))
          uncommittedConsumer.seekToBeginning(java.util.List.of(partition))
          assertEquals(
            pollValues(uncommittedConsumer, expected = 3),
            Vector("committed-before-restart", "aborted-before-restart", "committed-after-restart")
          )
        finally uncommittedConsumer.close()
      finally secondBroker.close()
    finally deleteTree(directory)
  }

  test("Kafka transactions gate the last stable offset and atomically commit consumer offsets") {
    val directory = Files.createTempDirectory("cascade-exactly-once-e2e")
    val broker = testBroker(directory)
    try
      broker.start()
      val admin = Admin.create(adminProperties(broker.bootstrapServers))
      try admin.createTopics(java.util.List.of(new NewTopic("transaction-output", 1, 1.toShort))).all().get()
      finally admin.close(Duration.ofSeconds(5))

      val partition = TopicPartition("transaction-output", 0)
      val groupId = "exactly-once-workers"
      val producer = KafkaProducer[Array[Byte], Array[Byte]](
        transactionalProducerProperties(broker.bootstrapServers, "exactly-once-producer")
      )
      try
        producer.initTransactions()
        producer.beginTransaction()
        producer.send(ProducerRecord("transaction-output", "committed".getBytes(StandardCharsets.UTF_8))).get()
        producer.sendOffsetsToTransaction(
          Map(partition -> OffsetAndMetadata(1L)).asJava,
          classicGroupMetadata(groupId)
        )

        val openTransactionReader = KafkaConsumer[Array[Byte], Array[Byte]](
          consumerProperties(broker.bootstrapServers, readCommitted = true)
        )
        try
          openTransactionReader.assign(java.util.List.of(partition))
          openTransactionReader.seekToBeginning(java.util.List.of(partition))
          assertEquals(openTransactionReader.endOffsets(java.util.List.of(partition)).get(partition).longValue(), 0L)
          assert(openTransactionReader.poll(Duration.ofMillis(300)).isEmpty)
        finally openTransactionReader.close()

        producer.commitTransaction()

        val groupConsumer = KafkaConsumer[Array[Byte], Array[Byte]](
          groupConsumerProperties(broker.bootstrapServers, groupId)
        )
        try
          val committed = groupConsumer.committed(java.util.Set.of(partition)).get(partition)
          assertEquals(Option(committed).map(_.offset()), Some(1L))
        finally groupConsumer.close()

        producer.beginTransaction()
        producer.send(ProducerRecord("transaction-output", "aborted".getBytes(StandardCharsets.UTF_8))).get()
        producer.sendOffsetsToTransaction(
          Map(partition -> OffsetAndMetadata(2L)).asJava,
          classicGroupMetadata(groupId)
        )
        producer.abortTransaction()

        val unchangedGroupConsumer = KafkaConsumer[Array[Byte], Array[Byte]](
          groupConsumerProperties(broker.bootstrapServers, groupId)
        )
        try
          val committed = unchangedGroupConsumer.committed(java.util.Set.of(partition)).get(partition)
          assertEquals(Option(committed).map(_.offset()), Some(1L))
        finally unchangedGroupConsumer.close()
      finally producer.close(Duration.ofSeconds(5))

      val committedReader = KafkaConsumer[Array[Byte], Array[Byte]](
        consumerProperties(broker.bootstrapServers, readCommitted = true)
      )
      try
        committedReader.assign(java.util.List.of(partition))
        committedReader.seekToBeginning(java.util.List.of(partition))
        assertEquals(pollValues(committedReader, expected = 1), Vector("committed"))
      finally committedReader.close()

      val uncommittedReader = KafkaConsumer[Array[Byte], Array[Byte]](
        consumerProperties(broker.bootstrapServers)
      )
      try
        uncommittedReader.assign(java.util.List.of(partition))
        uncommittedReader.seekToBeginning(java.util.List.of(partition))
        assertEquals(pollValues(uncommittedReader, expected = 2), Vector("committed", "aborted"))
      finally uncommittedReader.close()
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

  private def transactionalProducerProperties(bootstrapServers: String, transactionalId: String): Properties =
    val properties = producerProperties(bootstrapServers)
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    properties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "30000")
    properties

  @annotation.nowarn("cat=deprecation")
  private def classicGroupMetadata(groupId: String): ConsumerGroupMetadata =
    ConsumerGroupMetadata(groupId, -1, "", java.util.Optional.empty[String]())

  private def adminProperties(bootstrapServers: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties

  private def consumerProperties(bootstrapServers: String, readCommitted: Boolean = false): Properties =
    val properties = Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties.put(
      ConsumerConfig.ISOLATION_LEVEL_CONFIG,
      if readCommitted then "read_committed" else "read_uncommitted"
    )
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

  private def produceValue(
      bootstrapServers: String,
      topic: String,
      partition: Int,
      value: String,
      expectedOffset: Long
  ): Unit =
    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrapServers))
    try
      val metadata = producer.send(
        ProducerRecord[Array[Byte], Array[Byte]](
          topic,
          partition,
          null,
          value.getBytes(StandardCharsets.UTF_8)
        )
      ).get(15, TimeUnit.SECONDS)
      assertEquals(metadata.offset(), expectedOffset)
    finally producer.close(Duration.ofSeconds(5))

  private def awaitInSyncReplicas(
      admin: Admin,
      topic: String,
      partition: Int,
      expected: Set[Int]
  ): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var actual = Set.empty[Int]
    while actual != expected && System.nanoTime() < deadline do
      val description = admin.describeTopics(java.util.List.of(topic)).allTopicNames().get(5, TimeUnit.SECONDS).get(topic)
      actual = description.partitions().asScala
        .find(_.partition() == partition)
        .map(_.isr().asScala.map(_.id()).toSet)
        .getOrElse(Set.empty)
      if actual != expected then Thread.sleep(100L)
    assertEquals(actual, expected)

  private def awaitReplicaMembership(
      admin: Admin,
      topicPartition: TopicPartition,
      expectedReplicas: Set[Int],
      expectedInSync: Set[Int]
  ): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var replicas = Set.empty[Int]
    var inSync = Set.empty[Int]
    while (replicas != expectedReplicas || inSync != expectedInSync) && System.nanoTime() < deadline do
      val description = admin.describeTopics(java.util.List.of(topicPartition.topic()))
        .allTopicNames().get(5, TimeUnit.SECONDS).get(topicPartition.topic())
      description.partitions().asScala.find(_.partition() == topicPartition.partition()).foreach { partition =>
        replicas = partition.replicas().asScala.map(_.id()).toSet
        inSync = partition.isr().asScala.map(_.id()).toSet
      }
      if replicas != expectedReplicas || inSync != expectedInSync then Thread.sleep(100L)
    assertEquals(replicas, expectedReplicas)
    assertEquals(inSync, expectedInSync)

  private def awaitReassignment(admin: Admin, topicPartition: TopicPartition) =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var result: Option[org.apache.kafka.clients.admin.PartitionReassignment] = None
    while result.isEmpty && System.nanoTime() < deadline do
      result = Option(admin.listPartitionReassignments().reassignments().get(5, TimeUnit.SECONDS).get(topicPartition))
      if result.isEmpty then Thread.sleep(100L)
    result.getOrElse(fail(s"reassignment did not appear for $topicPartition"))

  private def awaitNoReassignment(admin: Admin, topicPartition: TopicPartition): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var present = true
    while present && System.nanoTime() < deadline do
      present = admin.listPartitionReassignments().reassignments().get(5, TimeUnit.SECONDS).containsKey(topicPartition)
      if present then Thread.sleep(100L)
    assert(!present, s"reassignment remained visible for $topicPartition")

  private def awaitController(admin: Admin, excludedId: Option[Int] = None): Int =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var controllerId = -1
    while (controllerId < 0 || excludedId.contains(controllerId)) && System.nanoTime() < deadline do
      try
        val candidate = admin.describeCluster().controller().get(5, TimeUnit.SECONDS)
        controllerId = Option(candidate).map(_.id()).getOrElse(-1)
      catch case _: Throwable => controllerId = -1
      if controllerId < 0 || excludedId.contains(controllerId) then Thread.sleep(100L)
    assert(controllerId >= 0 && !excludedId.contains(controllerId), s"controller election did not complete: $controllerId")
    controllerId

  private def controllerTerm(node: ClusterNode): Long =
    val (term, _) = controllerSnapshot(node)
    term

  private def clusterMetadata(node: ClusterNode): cascade.cluster.ClusterMetadata =
    val (_, metadata) = controllerSnapshot(node)
    metadata

  private def controllerSnapshot(node: ClusterNode): (Long, cascade.cluster.ClusterMetadata) =
    val peer = PeerClient()
    try
      val snapshot = peer.call(node, InternalApi.MetadataSnapshot, Array.emptyByteArray, 1000)
      val term = snapshot.readLong()
      snapshot.readInt()
      val metadata = MetadataCodec.decode(snapshot.readByteArray())
      snapshot.ensureFullyRead()
      (term, metadata)
    finally peer.close()

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

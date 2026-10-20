package cascade.fault

import cascade.cluster.InternalApi
import cascade.storage.{CleanupPolicy, StorageLifecycleConfig}
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class ReplicatedCleanupIntegrationSuite extends FunSuite:
  private val Topic = "replicated-cleanup-events"

  test("all ISR replicas apply one committed cleanup boundary") {
    val cluster = cleanupCluster()
    try
      cluster.startAll()
      val admin = Admin.create(adminProperties(cluster.bootstrapServers))
      try
        admin.createTopics(java.util.List.of(NewTopic(Topic, 1, 3.toShort))).all().get(20, TimeUnit.SECONDS)
        awaitIsr(admin)
        produce(cluster.bootstrapServers, 8)
        awaitCondition("replica cleanup convergence") {
          val bases = cluster.directories.map(segmentBases)
          bases.forall(_.headOption.exists(_ > 0L)) && bases.distinct.size == 1
        }
        val bases = cluster.directories.map(segmentBases)
        assertEquals(bases.distinct.size, 1)
        cluster.directories.foreach { directory =>
          assert(Files.exists(directory.resolve(Topic).resolve("partition-0").resolve("replicated-cleanup.checkpoint")))
        }
        val startOffset = readStartOffset(cluster.bootstrapServers)
        assert(startOffset > 0L)
        assert(startOffset >= bases.head.head)
      finally admin.close(Duration.ofSeconds(5))
    finally cluster.close()
  }

  test("a missing ISR prepare acknowledgement blocks physical deletion until healing") {
    val cluster = cleanupCluster()
    try
      cluster.startAll()
      val admin = Admin.create(adminProperties(cluster.bootstrapServers))
      try
        admin.createTopics(java.util.List.of(NewTopic(Topic, 1, 3.toShort))).all().get(20, TimeUnit.SECONDS)
        val metadata = awaitIsr(admin)
        val follower = (Set(1, 2, 3) - metadata.leader().id()).head
        val selector = FaultSelector(metadata.leader().id(), follower, Some(InternalApi.ReplicaCleanupPrepare))
        cluster.faults.block(selector)
        produce(cluster.bootstrapServers, 8)
        awaitCondition("blocked cleanup prepare attempt") {
          cluster.faults.calls.exists(call => selector.matches(call))
        }
        Thread.sleep(150L)
        assert(cluster.directories.forall(directory => segmentBases(directory).headOption.contains(0L)))

        cluster.faults.unblock(selector)
        awaitCondition("cleanup after prepare link healing") {
          val bases = cluster.directories.map(segmentBases)
          bases.forall(_.headOption.exists(_ > 0L)) && bases.distinct.size == 1
        }
      finally admin.close(Duration.ofSeconds(5))
    finally cluster.close()
  }

  private def cleanupCluster(): FaultCluster =
    FaultCluster(
      3,
      peerTimeoutMillis = 200,
      segmentBytes = 1024L,
      storageLifecycle = StorageLifecycleConfig(
        cleanupPolicy = CleanupPolicy.Delete,
        retentionMillis = -1L,
        retentionBytes = 1800L,
        lifecycleIntervalMillis = 25L
      )
    )

  private def produce(bootstrap: String, count: Int): Unit =
    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap))
    try
      (0 until count).foreach { index =>
        val value = Array.fill[Byte](700)((index + 1).toByte)
        producer.send(ProducerRecord(Topic, 0, index.toString.getBytes, value)).get(10, TimeUnit.SECONDS)
      }
      producer.flush()
    finally producer.close(Duration.ofSeconds(5))

  private def readStartOffset(bootstrap: String): Long =
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap))
    try
      val partition = TopicPartition(Topic, 0)
      consumer.assign(java.util.List.of(partition))
      consumer.seekToBeginning(java.util.List.of(partition))
      consumer.position(partition)
    finally consumer.close()

  private def segmentBases(directory: java.nio.file.Path): Vector[Long] =
    val partition = directory.resolve(Topic).resolve("partition-0")
    if !Files.isDirectory(partition) then Vector.empty
    else
      val paths = Files.list(partition)
      try paths.iterator().asScala
        .filter(path => path.getFileName.toString.matches("[0-9]{20}\\.log"))
        .flatMap { path =>
          try Option.when(Files.size(path) > 0L)(path.getFileName.toString.stripSuffix(".log").toLong)
          catch case _: java.nio.file.NoSuchFileException => None
        }
        .toVector
        .sorted
      finally paths.close()

  private def awaitIsr(admin: Admin): org.apache.kafka.common.TopicPartitionInfo =
    var result: Option[org.apache.kafka.common.TopicPartitionInfo] = None
    awaitCondition("three-member ISR") {
      val info = admin.describeTopics(java.util.List.of(Topic)).allTopicNames().get(5, TimeUnit.SECONDS)
        .get(Topic).partitions().get(0)
      result = Some(info)
      info.isr().asScala.map(_.id()).toSet == Set(1, 2, 3)
    }
    result.get

  private def awaitCondition(description: String)(condition: => Boolean): Unit =
    val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L)
    var satisfied = condition
    while !satisfied && System.nanoTime() < deadline do
      Thread.sleep(20L)
      satisfied = condition
    assert(satisfied, s"timed out waiting for $description")

  private def adminProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties

  private def producerProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    properties

  private def consumerProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties

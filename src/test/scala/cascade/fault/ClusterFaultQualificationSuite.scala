package cascade.fault

import java.nio.charset.StandardCharsets
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

final class ClusterFaultQualificationSuite extends FunSuite:
  test("a majority survives an active-controller network partition and heals without data loss") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      val admin = Admin.create(adminProperties(cluster.bootstrapServers))
      try
        admin.createTopics(java.util.List.of(NewTopic("partition-events", 3, 3.toShort))).all().get(20, TimeUnit.SECONDS)
        val firstController = awaitController(admin)
        val partition = firstController - 1
        awaitInSyncReplicas(admin, "partition-events", partition, Set(1, 2, 3))
        produce(cluster.bootstrapServers, "partition-events", partition, "before-partition", 0L)

        val majority = Set(1, 2, 3) - firstController
        cluster.faults.partition(Set(firstController), majority)
        val majorityBootstrap = cluster.nodes.filter(node => majority(node.id))
          .map(node => s"${node.host}:${node.port}").mkString(",")
        val majorityAdmin = Admin.create(adminProperties(majorityBootstrap))
        try
          val nextController = awaitController(majorityAdmin, excluded = Some(firstController))
          assert(majority(nextController))
          awaitInSyncReplicas(majorityAdmin, "partition-events", partition, majority)
          produce(majorityBootstrap, "partition-events", partition, "during-partition", 1L)
        finally majorityAdmin.close(Duration.ofSeconds(5))

        cluster.faults.heal()
        awaitInSyncReplicas(admin, "partition-events", partition, Set(1, 2, 3))
        val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(cluster.bootstrapServers))
        try
          val topicPartition = TopicPartition("partition-events", partition)
          consumer.assign(java.util.List.of(topicPartition))
          consumer.seekToBeginning(java.util.List.of(topicPartition))
          assertEquals(pollValues(consumer, 2), Vector("before-partition", "during-partition"))
        finally consumer.close()
      finally admin.close(Duration.ofSeconds(5))
    finally cluster.close()
  }

  private def produce(bootstrap: String, topic: String, partition: Int, value: String, expectedOffset: Long): Unit =
    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap))
    try
      val result = producer.send(
        ProducerRecord(topic, partition, null, value.getBytes(StandardCharsets.UTF_8))
      ).get(20, TimeUnit.SECONDS)
      assertEquals(result.offset(), expectedOffset)
    finally producer.close(Duration.ofSeconds(5))

  private def pollValues(consumer: KafkaConsumer[Array[Byte], Array[Byte]], expected: Int): Vector[String] =
    val values = Vector.newBuilder[String]
    var count = 0
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    while count < expected && System.nanoTime() < deadline do
      consumer.poll(Duration.ofMillis(250)).iterator().asScala.foreach { record =>
        values += String(record.value(), StandardCharsets.UTF_8)
        count += 1
      }
    val result = values.result()
    assertEquals(result.size, expected)
    result

  private def awaitController(admin: Admin, excluded: Option[Int] = None): Int =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var controller = -1
    while (controller < 0 || excluded.contains(controller)) && System.nanoTime() < deadline do
      try controller = Option(admin.describeCluster().controller().get(3, TimeUnit.SECONDS)).map(_.id()).getOrElse(-1)
      catch case _: Throwable => controller = -1
      if controller < 0 || excluded.contains(controller) then Thread.sleep(100L)
    assert(controller >= 0 && !excluded.contains(controller), s"controller election did not complete: $controller")
    controller

  private def awaitInSyncReplicas(admin: Admin, topic: String, partition: Int, expected: Set[Int]): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var actual = Set.empty[Int]
    while actual != expected && System.nanoTime() < deadline do
      try
        actual = admin.describeTopics(java.util.List.of(topic)).allTopicNames().get(3, TimeUnit.SECONDS).get(topic)
          .partitions().asScala.find(_.partition() == partition)
          .map(_.isr().asScala.map(_.id()).toSet).getOrElse(Set.empty)
      catch case _: Throwable => actual = Set.empty
      if actual != expected then Thread.sleep(100L)
    assertEquals(actual, expected)

  private def adminProperties(bootstrap: String): Properties =
    val values = Properties()
    values.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    values.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    values.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    values

  private def producerProperties(bootstrap: String): Properties =
    val values = Properties()
    values.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    values.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    values.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    values.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    values.put(ProducerConfig.ACKS_CONFIG, "all")
    values.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "15000")
    values.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    values

  private def consumerProperties(bootstrap: String): Properties =
    val values = Properties()
    values.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    values.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    values.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    values.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    values.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    values.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    values.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    values

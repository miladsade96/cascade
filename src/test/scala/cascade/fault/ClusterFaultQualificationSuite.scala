package cascade.fault

import cascade.cluster.{InternalApi, MetadataCodec, PeerClient}
import cascade.protocol.{ApiKey, ByteCursor, ByteWriter, Errors}
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic, RaftVoterEndpoint}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{TopicPartition, Uuid}
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
          val nextController = awaitControllerNodes(cluster.nodes.filter(node => majority(node.id)), Some(firstController))
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

  test("an isolated coordinator cannot acknowledge offsets that the majority never committed") {
    val cluster = FaultCluster(3)
    val topic = "coordinator-partition-events"
    val groupId = "partitioned-workers"
    val topicPartition = TopicPartition(topic, 0)
    try
      cluster.startAll()
      val admin = Admin.create(adminProperties(cluster.bootstrapServers))
      try
        admin.createTopics(java.util.List.of(NewTopic(topic, 1, 3.toShort))).all().get(20, TimeUnit.SECONDS)
        awaitInSyncReplicas(admin, topic, 0, Set(1, 2, 3))
        produce(cluster.bootstrapServers, topic, 0, "committed", 0L)

        val initial = KafkaConsumer[Array[Byte], Array[Byte]](groupConsumerProperties(cluster.bootstrapServers, groupId))
        try
          initial.assign(java.util.List.of(topicPartition))
          initial.seekToBeginning(java.util.List.of(topicPartition))
          assertEquals(pollValues(initial, 1), Vector("committed"))
          initial.commitSync()
        finally initial.close()

        val firstController = awaitController(admin)
        val majority = Set(1, 2, 3) - firstController
        cluster.faults.partition(Set(firstController), majority)
        val majorityBootstrap = cluster.nodes.filter(node => majority(node.id))
          .map(node => s"${node.host}:${node.port}").mkString(",")
        val majorityAdmin = Admin.create(adminProperties(majorityBootstrap))
        try awaitControllerNodes(cluster.nodes.filter(node => majority(node.id)), Some(firstController))
        finally majorityAdmin.close(Duration.ofSeconds(5))

        val isolatedBootstrap = {
          val node = cluster.nodes(firstController - 1)
          s"${node.host}:${node.port}"
        }
        Thread.sleep(750L)
        assertEquals(offsetCommitError(isolatedBootstrap, groupId, topicPartition, 99L), Errors.NotCoordinator)

        val majorityReader = KafkaConsumer[Array[Byte], Array[Byte]](groupConsumerProperties(majorityBootstrap, groupId))
        try
          assertEquals(
            Option(majorityReader.committed(java.util.Set.of(topicPartition)).get(topicPartition)).map(_.offset()),
            Some(1L)
          )
          majorityReader.assign(java.util.List.of(topicPartition))
          majorityReader.commitSync(Map(topicPartition -> OffsetAndMetadata(2L)).asJava)
        finally majorityReader.close()

        cluster.faults.heal()
        val healed = KafkaConsumer[Array[Byte], Array[Byte]](groupConsumerProperties(cluster.bootstrapServers, groupId))
        try
          assertEquals(
            Option(healed.committed(java.util.Set.of(topicPartition)).get(topicPartition)).map(_.offset()),
            Some(2L)
          )
        finally healed.close()
      finally admin.close(Duration.ofSeconds(5))
    finally cluster.close()
  }

  test("a joint voter transition remains durable when stabilization is partitioned and resumes after healing") {
    val cluster = FaultCluster(size = 4, initialVoters = 3)
    try
      cluster.startAll()
      val admin = Admin.create(adminProperties(cluster.bootstrapServers))
      try
        val controller = awaitController(admin)
        val armed = ArmedFault(
          triggerMatches = 2,
          trigger = call =>
            call.sourceId == controller && call.apiKey == InternalApi.MetadataCommit && metadataFromCommit(call).exists {
              _.membership.exists(_.isJoint)
            },
          drop = call => call.sourceId == controller && call.apiKey == InternalApi.MetadataPrepare
        )
        cluster.faults.arm(armed)
        val directoryId = Uuid.randomUuid()
        intercept[Throwable] {
          admin.addRaftVoter(
            4,
            directoryId,
            Set(RaftVoterEndpoint("CONTROLLER", cluster.nodes(3).host, cluster.nodes(3).port)).asJava
          ).all().get(10, TimeUnit.SECONDS)
        }
        assert(armed.isArmed)

        val joint = metadataSnapshot(cluster.nodes(controller - 1))
        assert(joint.membership.exists(_.isJoint))
        assertEquals(joint.membership.get.targetVoters.map(_.id).toSet, Set(1, 2, 3, 4))

        cluster.faults.heal()
        awaitVoters(admin, Set(1, 2, 3, 4))
        awaitStableMembership(cluster.nodes(controller - 1), Set(1, 2, 3, 4))
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

  private def offsetCommitError(
      bootstrap: String,
      groupId: String,
      topicPartition: TopicPartition,
      offset: Long
  ): Short =
    val endpoint = bootstrap.split(':')
    val socket = Socket(endpoint(0), endpoint(1).toInt)
    try
      val input = DataInputStream(BufferedInputStream(socket.getInputStream))
      val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
      val writer = ByteWriter()
        .writeShort(ApiKey.OffsetCommit)
        .writeShort(7)
        .writeInt(77)
        .writeNullableString(Some("fault-qualification"))
        .writeString(groupId)
        .writeInt(-1)
        .writeString("")
        .writeNullableString(None)
      writer.writeArray(Vector(topicPartition.topic())) { topic =>
        writer.writeString(topic)
        writer.writeArray(Vector(topicPartition.partition())) { partition =>
          writer.writeInt(partition).writeLong(offset).writeInt(-1).writeNullableString(None): Unit
        }: Unit
      }
      val payload = writer.result()
      output.writeInt(payload.length)
      output.write(payload)
      output.flush()
      val response = new Array[Byte](input.readInt())
      input.readFully(response)
      val cursor = ByteCursor(response)
      assertEquals(cursor.readInt(), 77)
      cursor.readInt()
      cursor.readInt()
      cursor.readString()
      cursor.readInt()
      cursor.readInt()
      val error = cursor.readShort()
      cursor.ensureFullyRead()
      error
    finally socket.close()

  private def metadataFromCommit(call: PeerCall): Option[cascade.cluster.ClusterMetadata] =
    try
      val cursor = ByteCursor(call.payload.toArray)
      cursor.readLong()
      cursor.readInt()
      val metadata = MetadataCodec.decode(cursor.readByteArray())
      cursor.ensureFullyRead()
      Some(metadata)
    catch case _: Throwable => None

  private def metadataSnapshot(node: cascade.cluster.ClusterNode): cascade.cluster.ClusterMetadata =
    val peer = PeerClient()
    try
      val response = peer.call(node, InternalApi.MetadataSnapshot, Array.emptyByteArray, 1000)
      response.readLong()
      response.readInt()
      val metadata = MetadataCodec.decode(response.readByteArray())
      response.ensureFullyRead()
      metadata
    finally peer.close()

  private def awaitControllerNodes(
      nodes: Vector[cascade.cluster.ClusterNode],
      excluded: Option[Int]
  ): Int =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var controller = -1
    while (controller < 0 || excluded.contains(controller)) && System.nanoTime() < deadline do
      controller = nodes.iterator.flatMap { node =>
        val peer = PeerClient()
        try
          val response = peer.call(node, InternalApi.MetadataSnapshot, Array.emptyByteArray, 1000)
          response.readLong()
          val leaderId = response.readInt()
          response.readByteArray()
          response.ensureFullyRead()
          Option.when(leaderId >= 0 && !excluded.contains(leaderId))(leaderId)
        catch case _: Throwable => None
        finally peer.close()
      }.nextOption().getOrElse(-1)
      if controller < 0 || excluded.contains(controller) then Thread.sleep(100L)
    assert(controller >= 0 && !excluded.contains(controller), s"controller election did not complete: $controller")
    controller

  private def awaitStableMembership(node: cascade.cluster.ClusterNode, expected: Set[Int]): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var membership: Option[cascade.cluster.QuorumMembership] = None
    while membership.forall(value => value.isJoint || value.currentVoters.map(_.id).toSet != expected) &&
        System.nanoTime() < deadline
    do
      try membership = metadataSnapshot(node).membership
      catch case _: Throwable => membership = None
      if membership.forall(value => value.isJoint || value.currentVoters.map(_.id).toSet != expected) then Thread.sleep(100L)
    assert(membership.exists(value => !value.isJoint && value.currentVoters.map(_.id).toSet == expected))

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

  private def awaitVoters(admin: Admin, expected: Set[Int]): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var actual = Set.empty[Int]
    while actual != expected && System.nanoTime() < deadline do
      try
        actual = admin.describeMetadataQuorum().quorumInfo().get(3, TimeUnit.SECONDS)
          .voters().asScala.map(_.replicaId()).toSet
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

  private def groupConsumerProperties(bootstrap: String, groupId: String): Properties =
    val values = consumerProperties(bootstrap)
    values.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    values.put("group.protocol", "classic")
    values.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "6000")
    values.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000")
    values

package cascade.broker

import cascade.coordinator.CoordinatorProbe
import cascade.fault.FaultCluster
import cascade.protocol.{ByteWriter, Errors}
import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.ByteBuffer
import java.nio.file.Files
import org.apache.kafka.common.message.{MetadataRequestData, MetadataResponseData}
import org.apache.kafka.common.protocol.{ByteBufferAccessor, MessageUtil}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

/** Decode every advertised version using Kafka's generated schema, not Cascade's own cursor. */
final class MetadataVersionIntegrationSuite extends FunSuite:
  private def metadata(port: Int, version: Short, names: Vector[String], autoCreate: Boolean): MetadataResponseData =
    val data = MetadataRequestData().setAllowAutoTopicCreation(autoCreate)
      .setTopics(names.map(name => MetadataRequestData.MetadataRequestTopic().setName(name)).asJava)
    val body = MessageUtil.toByteBufferAccessor(data, version).buffer()
    val header = ByteWriter().writeShort(3).writeShort(version).writeInt(version.toInt)
      .writeNullableString(Some("metadata-version-test"))
    if version >= 9 then header.writeEmptyTaggedFields()
    val bytes = header.writeBytes(MessageUtil.byteBufferToArray(body)).result()
    val socket = Socket("127.0.0.1", port)
    socket.setSoTimeout(5000)
    try
      val output = DataOutputStream(socket.getOutputStream)
      output.writeInt(bytes.length)
      output.write(bytes)
      output.flush()
      val input = DataInputStream(socket.getInputStream)
      val buffer = ByteBuffer.wrap(input.readNBytes(input.readInt()))
      assertEquals(buffer.getInt(), version.toInt)
      if version >= 9 then assertEquals(buffer.get(), 0.toByte)
      val decoded = MetadataResponseData(ByteBufferAccessor(buffer), version)
      assertEquals(buffer.remaining(), 0, s"unread bytes in Metadata v$version")
      decoded
    finally socket.close()

  private def verifyVersions(port: Int, topic: String, replicas: Int): Unit =
    (4 to 12).foreach { number =>
      val response = metadata(port, number.toShort, Vector(topic, "does-not-exist"), autoCreate = false)
      assertEquals(response.brokers().size(), replicas)
      assertEquals(response.topics().size(), 2)
      val existing = response.topics().asScala.find(_.name() == topic).get
      assertEquals(existing.errorCode(), Errors.None)
      assertEquals(existing.partitions().size(), 1)
      val partition = existing.partitions().get(0)
      assertEquals(partition.errorCode(), Errors.None)
      assertEquals(partition.partitionIndex(), 0)
      assert(partition.leaderId() >= 1)
      assertEquals(partition.replicaNodes().size(), replicas)
      assertEquals(partition.isrNodes().size(), replicas)
      assertEquals(partition.offlineReplicas().size(), 0)
      if number >= 7 then assert(partition.leaderEpoch() >= 0)
      val missing = response.topics().asScala.find(_.name() == "does-not-exist").get
      assertEquals(missing.errorCode(), Errors.UnknownTopicOrPartition)
      assertEquals(missing.partitions().size(), 0)
    }

  test("standalone Metadata versions 4 through 12 match Kafka's generated response schema") {
    val directory = Files.createTempDirectory("cascade-metadata-versions")
    val broker = KafkaBroker(BrokerConfig(bindHost = "127.0.0.1", port = 0,
      advertisedHost = "127.0.0.1", dataDirectory = directory))
    try
      broker.start()
      metadata(broker.boundPort, 4, Vector("versioned-topic"), autoCreate = true)
      verifyVersions(broker.boundPort, "versioned-topic", 1)
    finally
      broker.close()
      val paths = Files.walk(directory)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()
  }

  test("cluster Metadata versions 4 through 12 match Kafka's generated response schema") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      verifyVersions(CoordinatorProbe.controller(cluster.nodes).port, "coordinator-qualification", 3)
    finally cluster.close()
  }

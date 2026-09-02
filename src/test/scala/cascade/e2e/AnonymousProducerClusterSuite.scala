package cascade.e2e

import cascade.cluster.ClusterNode
import cascade.coordinator.CoordinatorProbe
import cascade.fault.FaultCluster
import cascade.protocol.{ByteCursor, ByteWriter, Errors}
import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import munit.FunSuite

final class AnonymousProducerClusterSuite extends FunSuite:
  private def allocate(node: ClusterNode): (Short, Long) =
    val socket = Socket(node.host, node.port)
    socket.setSoTimeout(5000)
    try
      val request = ByteWriter().writeShort(22).writeShort(1).writeInt(1).writeNullableString(Some("allocator-test"))
        .writeNullableString(None).writeInt(30000).result()
      val output = DataOutputStream(socket.getOutputStream)
      output.writeInt(request.length)
      output.write(request)
      output.flush()
      val input = DataInputStream(socket.getInputStream)
      val response = ByteCursor(input.readNBytes(input.readInt()))
      assertEquals(response.readInt(), 1)
      response.readInt()
      val result = response.readShort() -> response.readLong()
      response.readShort()
      response.ensureFullyRead()
      result
    finally socket.close()

  test("each broker allocates unique anonymous IDs and quorum loss returns a safe retry error") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val ids = cluster.nodes.flatMap { node =>
        (1 to 3).map { _ =>
          val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L)
          var result = allocate(node)
          while result._1 == Errors.CoordinatorLoadInProgress && System.nanoTime() < deadline do
            Thread.sleep(25L)
            result = allocate(node)
          assertEquals(result._1, Errors.None)
          result._2
        }
      }
      assertEquals(ids.distinct.size, 9)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      cluster.faults.partition(Set(controller.id), cluster.nodes.map(_.id).toSet - controller.id)
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L)
      while !cluster.broker(controller.id).metricsSnapshot.brokerFenced && System.nanoTime() < deadline do Thread.sleep(25L)
      assertEquals(allocate(controller), Errors.CoordinatorLoadInProgress -> -1L)
    finally cluster.close()
  }

  test("Kafka idempotent producers bootstrap from every broker") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      cluster.nodes.zipWithIndex.foreach { case (node, index) =>
        val properties = Properties()
        properties.setProperty("bootstrap.servers", s"${node.host}:${node.port}")
        properties.setProperty("key.serializer", classOf[ByteArraySerializer].getName)
        properties.setProperty("value.serializer", classOf[ByteArraySerializer].getName)
        properties.setProperty("enable.idempotence", "true")
        properties.setProperty("acks", "all")
        properties.setProperty("request.timeout.ms", "5000")
        properties.setProperty("delivery.timeout.ms", "20000")
        val producer = KafkaProducer[Array[Byte], Array[Byte]](properties)
        try
          val result = producer.send(ProducerRecord("coordinator-qualification", 0, null, Array(index.toByte))).get(25L, TimeUnit.SECONDS)
          assertEquals(result.offset(), index.toLong)
        finally producer.close(java.time.Duration.ofSeconds(5L))
      }
    finally cluster.close()
  }

package cascade.cluster

import cascade.coordinator.{CoordinatorKey, CoordinatorProbe}
import cascade.fault.{FaultCluster, FaultSelector, PeerPause}
import cascade.group.OffsetBatchConfig
import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.jdk.CollectionConverters.*
import munit.FunSuite

final class CoordinatorReadIsolationQuorumSuite extends FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(90L, "seconds")

  test("OffsetFetch serves the acknowledged value while owner publication is paused") {
    val cluster = FaultCluster(
      3,
      peerTimeoutMillis = 5000,
      heartbeatMillis = 250,
      electionTimeoutMillis = 10000,
      offsetBatch = OffsetBatchConfig(maxRequests = 1, lingerMillis = 0L)
    )
    val executor = Executors.newSingleThreadExecutor()
    val partition = TopicPartition("read-isolation-qualification", 0)
    var writer: KafkaConsumer[Array[Byte], Array[Byte]] | Null = null
    var reader: KafkaConsumer[Array[Byte], Array[Byte]] | Null = null
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val group = Iterator.from(0).map(index => s"read-isolation-$index")
        .find(value => CoordinatorRouting.owner(CoordinatorKey.group(value).routingKey, cluster.nodes).exists(_.id != controller.id)).get
      val owner = CoordinatorRouting.owner(CoordinatorKey.group(group).routingKey, cluster.nodes).get
      val follower = cluster.nodes.find(_.id != owner.id).get
      createTopic(cluster.bootstrapServers, partition.topic())
      writer = consumer(cluster.bootstrapServers, group)
      reader = consumer(cluster.bootstrapServers, group)
      writer.nn.assign(java.util.List.of(partition))
      reader.nn.assign(java.util.List.of(partition))
      writer.nn.commitSync(Map(partition -> OffsetAndMetadata(10L)).asJava)
      assertEquals(readOffset(reader.nn, partition), 10L)

      val entered = CountDownLatch(1)
      val release = CountDownLatch(1)
      val selector = FaultSelector(owner.id, follower.id, Some(InternalApi.CoordinatorShardFinalize))
      cluster.faults.pause(PeerPause(selector, entered, release, 5000L))
      val before = cluster.broker(owner.id).metricsSnapshot.coordinatorReads.offsetSnapshots
      val write = executor.submit[Unit](() =>
        writer.nn.commitSync(Map(partition -> OffsetAndMetadata(20L)).asJava)
      )
      assert(entered.await(5L, TimeUnit.SECONDS), "owner did not reach the paused shard finalization")

      val started = System.nanoTime()
      assertEquals(readOffset(reader.nn, partition), 10L)
      val elapsedMillis = (System.nanoTime() - started) / 1_000_000L
      assert(elapsedMillis < 2000L, s"OffsetFetch waited behind coordinator publication for $elapsedMillis ms")
      assert(cluster.broker(owner.id).metricsSnapshot.coordinatorReads.offsetSnapshots > before)

      cluster.faults.resume(selector)
      write.get(10L, TimeUnit.SECONDS)
      assertEquals(readOffset(reader.nn, partition), 20L)
    finally
      cluster.faults.heal()
      Option(writer).foreach(_.close())
      Option(reader).foreach(_.close())
      executor.shutdownNow(): Unit
      executor.awaitTermination(10L, TimeUnit.SECONDS): Unit
      cluster.close()
  }

  private def readOffset(client: KafkaConsumer[Array[Byte], Array[Byte]], partition: TopicPartition): Long =
    Option(client.committed(java.util.Set.of(partition)).get(partition)).map(_.offset()).getOrElse(-1L)

  private def consumer(bootstrap: String, group: String): KafkaConsumer[Array[Byte], Array[Byte]] =
    val properties = Properties()
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", group)
    properties.setProperty("group.protocol", "classic")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.setProperty("default.api.timeout.ms", "10000")
    properties.setProperty("request.timeout.ms", "5000")
    properties.setProperty("enable.metrics.push", "false")
    KafkaConsumer[Array[Byte], Array[Byte]](properties)

  private def createTopic(bootstrap: String, name: String): Unit =
    val properties = Properties()
    properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.setProperty(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "20000")
    properties.setProperty(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    val admin = Admin.create(properties)
    try admin.createTopics(java.util.List.of(NewTopic(name, 1, 3.toShort))).all().get(20L, TimeUnit.SECONDS): Unit
    finally admin.close()

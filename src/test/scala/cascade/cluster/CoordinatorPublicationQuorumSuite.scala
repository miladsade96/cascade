package cascade.cluster

import cascade.coordinator.{CoordinatorKey, CoordinatorProbe, CoordinatorPublicationConfig, CoordinatorShard}
import cascade.fault.FaultCluster
import cascade.group.OffsetBatchConfig
import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.jdk.CollectionConverters.*
import munit.FunSuite

final class CoordinatorPublicationQuorumSuite extends FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(90L, "seconds")

  test("commits disjoint owner proposals through shard quorums and recovers exactly") {
    val cluster = FaultCluster(
      3,
      peerTimeoutMillis = 3000,
      heartbeatMillis = 250,
      electionTimeoutMillis = 10000,
      offsetBatch = OffsetBatchConfig(maxRequests = 1, lingerMillis = 0L),
      coordinatorPublication = CoordinatorPublicationConfig(maxRequests = 16, lingerMillis = 50L, queueTimeoutMillis = 5000L)
    )
    val executor = Executors.newFixedThreadPool(12)
    val partition = TopicPartition("publication-qualification", 0)
    var clients = Vector.empty[KafkaConsumer[Array[Byte], Array[Byte]]]
    var tasks = Vector.empty[java.util.concurrent.Future[Unit]]
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      CoordinatorProbe.controller(cluster.nodes)
      createTopic(cluster.bootstrapServers, partition.topic())
      val before = cluster.nodes.map(node => node.id -> cluster.broker(node.id).metricsSnapshot.coordinatorQuorum).toMap
      val groups = selectGroups(cluster, 12)
      clients = groups.map(group => consumer(cluster.bootstrapServers, group))
      clients.foreach(_.assign(java.util.List.of(partition)))
      val start = CountDownLatch(1)
      tasks = clients.zipWithIndex.map { case (client, index) =>
        executor.submit[Unit](() =>
          start.await()
          client.commitSync(Map(partition -> OffsetAndMetadata(index.toLong + 1L)).asJava)
        )
      }
      start.countDown()
      tasks.foreach(_.get(20L, TimeUnit.SECONDS))
      tasks = Vector.empty
      clients.zipWithIndex.foreach { case (client, index) =>
        assertEquals(client.committed(java.util.Set.of(partition)).get(partition).offset(), index.toLong + 1L)
      }
      val quorum = cluster.nodes.map(node => cluster.broker(node.id).metricsSnapshot.coordinatorQuorum)
      assertEquals(quorum.map(_.committed).sum - before.values.map(_.committed).sum, 12L)
      assertEquals(quorum.map(_.failed).sum - before.values.map(_.failed).sum, 0L)
      assertEquals(quorum.map(_.rejected).sum - before.values.map(_.rejected).sum, 0L)
      assertEquals(quorum.map(_.committed), Vector(4L, 4L, 4L))
      assert(quorum.forall(value => value.peakInflight >= 1 && value.peakInflight <= 256), quorum)
      assertEquals(quorum.map(_.store.pending).sum, 0)
      assert(quorum.map(_.store.journalRecords).sum >= 12L * 3L * 3L, quorum)

      clients.foreach(_.close())
      clients = Vector.empty
      cluster.nodes.foreach(node => cluster.stop(node.id))
      cluster.startAll()
      CoordinatorProbe.controller(cluster.nodes)
      groups.zipWithIndex.foreach { case (group, index) =>
        val client = consumer(cluster.bootstrapServers, group)
        try
          client.assign(java.util.List.of(partition))
          assertEquals(client.committed(java.util.Set.of(partition)).get(partition).offset(), index.toLong + 1L)
        finally client.close()
      }
    finally
      tasks.foreach(_.cancel(true): Unit)
      executor.shutdownNow(): Unit
      executor.awaitTermination(10L, TimeUnit.SECONDS): Unit
      clients.foreach(_.close())
      cluster.close()
  }

  private def selectGroups(cluster: FaultCluster, count: Int): Vector[String] =
    val selected = Vector.newBuilder[String]
    val shards = scala.collection.mutable.HashSet.empty[Int]
    val owners = scala.collection.mutable.HashMap.empty[Int, Int].withDefaultValue(0)
    Iterator.from(0).map(index => s"publication-group-$index").takeWhile { group =>
      if selected.result().size >= count then false
      else
        val shard = CoordinatorShard.group(group)
        val owner = CoordinatorRouting.owner(CoordinatorKey.group(group).routingKey, cluster.nodes).map(_.id).get
        if !shards(shard) && owners(owner) < count / cluster.nodes.size then
          selected += group
          shards += shard
          owners.update(owner, owners(owner) + 1)
        true
    }.foreach(_ => ())
    val result = selected.result()
    require(result.size == count && owners.values.sum == count, s"could not distribute publication groups: $owners")
    result

  private def consumer(bootstrap: String, group: String): KafkaConsumer[Array[Byte], Array[Byte]] =
    val properties = Properties()
    properties.setProperty("bootstrap.servers", bootstrap)
    properties.setProperty("group.id", group)
    properties.setProperty("group.protocol", "classic")
    properties.setProperty("enable.auto.commit", "false")
    properties.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
    properties.setProperty("default.api.timeout.ms", "20000")
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

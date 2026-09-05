package cascade.cluster

import cascade.coordinator.{CoordinatorProbe, CoordinatorPublicationConfig, CoordinatorShard}
import cascade.fault.FaultCluster
import cascade.group.OffsetBatchConfig
import java.util.Properties
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import scala.jdk.CollectionConverters.*
import munit.FunSuite

final class CoordinatorPublicationQuorumSuite extends FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(90L, "seconds")

  test("coalesces disjoint owner proposals into fewer quorum publications and recovers exactly") {
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
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val groups = selectGroups(cluster, 12)
      clients = groups.map(group => consumer(cluster.bootstrapServers, group))
      clients.foreach(_.assign(java.util.List.of(partition)))
      val start = CountDownLatch(1)
      val results = clients.zipWithIndex.map { case (client, index) =>
        executor.submit[Unit](() =>
          start.await()
          client.commitSync(Map(partition -> OffsetAndMetadata(index.toLong + 1L)).asJava)
        )
      }
      start.countDown()
      results.foreach(_.get(20L, TimeUnit.SECONDS))
      clients.zipWithIndex.foreach { case (client, index) =>
        assertEquals(client.committed(java.util.Set.of(partition)).get(partition).offset(), index.toLong + 1L)
      }
      val publication = cluster.broker(controller.id).metricsSnapshot.coordinatorPublication
      assertEquals(publication.committedRequests, 12L)
      assertEquals(publication.failed, 0L)
      assertEquals(publication.conflictedRequests, 0L)
      assert(publication.committedBatches < publication.committedRequests, publication)
      assert(publication.peakRequests <= 16, publication)

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
      clients.foreach(_.close())
      executor.shutdownNow(): Unit
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
        val owner = CoordinatorRouting.owner(group, cluster.nodes).map(_.id).get
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

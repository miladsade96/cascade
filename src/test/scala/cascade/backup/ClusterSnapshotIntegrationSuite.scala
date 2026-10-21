package cascade.backup

import cascade.coordinator.CoordinatorProbe
import cascade.fault.FaultCluster
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer

final class ClusterSnapshotIntegrationSuite extends FunSuite:
  test("three voters publish one fenced and independently verifiable cluster snapshot") {
    val cluster = FaultCluster(3, peerTimeoutMillis = 3000, electionTimeoutMillis = 10000)
    val destination = Files.createTempDirectory("cascade-cluster-snapshot-destination").resolve("generation-1")
    try
      cluster.startAll()
      val admin = Admin.create(adminProperties(cluster.bootstrapServers))
      try
        admin.createTopics(java.util.List.of(NewTopic("snapshot-events", 1, 3.toShort))).all().get(20L, TimeUnit.SECONDS)
        awaitFullIsr(admin, "snapshot-events")
        produce(cluster.bootstrapServers, 50)
        val controller = CoordinatorProbe.controller(cluster.nodes)
        val manifest = cluster.broker(controller.id).createClusterSnapshot(destination)
        assertEquals(manifest.nodes.map(_.nodeId).sorted, Vector(1, 2, 3))
        assertEquals(ClusterSnapshotManifestCodec.read(destination.resolve(ClusterSnapshotManifestCodec.FileName)), manifest)
        manifest.nodes.foreach { artifact =>
          val nodeDirectory = destination.resolve(artifact.artifactDirectory)
          val verified = BackupRestore.verify(nodeDirectory)
          assertEquals(verified.manifest.entries.size, artifact.fileCount)
          assertEquals(verified.totalBytes, artifact.totalBytes)
          assertEquals(Sha256.file(nodeDirectory.resolve(BackupManifest.FileName)), artifact.manifestSha256)
        }
        val eventWatermarks = manifest.nodes.flatMap(_.watermarks.filter(value =>
          value.topic == "snapshot-events" && value.partition == 0
        )).map(_.highWatermark)
        assertEquals(eventWatermarks.size, 3)
        assertEquals(eventWatermarks.distinct, Vector(50L))
        produce(cluster.bootstrapServers, 5, start = 50)
      finally admin.close(Duration.ofSeconds(5))
    finally
      cluster.close()
      deleteTree(Option(destination.getParent).get)
  }

  private def awaitFullIsr(admin: Admin, topic: String): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var ready = false
    while !ready && System.nanoTime() < deadline do
      val description = admin.describeTopics(java.util.List.of(topic)).allTopicNames().get(5L, TimeUnit.SECONDS).get(topic)
      ready = description.partitions().get(0).isr().size() == 3
      if !ready then Thread.sleep(25L)
    assert(ready, "topic did not reach a full ISR")

  private def produce(bootstrap: String, count: Int, start: Int = 0): Unit =
    val properties = Properties()
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    val producer = KafkaProducer[Array[Byte], Array[Byte]](properties)
    try
      (start until start + count).foreach { index =>
        producer.send(ProducerRecord("snapshot-events", s"value-$index".getBytes(StandardCharsets.UTF_8)))
          .get(10L, TimeUnit.SECONDS): Unit
      }
    finally producer.close(Duration.ofSeconds(5))

  private def adminProperties(bootstrap: String): Properties =
    val properties = Properties()
    properties.setProperty("bootstrap.servers", bootstrap)
    properties

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path): Unit)
      finally paths.close()

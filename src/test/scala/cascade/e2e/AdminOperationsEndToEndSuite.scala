package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, ConfigEntry, NewTopic}
import org.apache.kafka.common.config.ConfigResource
import scala.jdk.CollectionConverters.*

final class AdminOperationsEndToEndSuite extends munit.FunSuite:
  test("Apache Kafka Admin reads broker and topic configuration from Cascade") {
    val directory = Files.createTempDirectory("cascade-admin-config-e2e")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory,
        segmentBytes = 8L * 1024 * 1024,
        defaultReplicationFactor = 1,
        minInSyncReplicas = 1
      )
    )
    try
      broker.start()
      val properties = Properties()
      properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
      properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
      val admin = Admin.create(properties)
      try
        admin.createTopics(java.util.List.of(NewTopic("admin-visible", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
        val brokerResource = ConfigResource(ConfigResource.Type.BROKER, broker.config.nodeId.toString)
        val topicResource = ConfigResource(ConfigResource.Type.TOPIC, "admin-visible")
        val described = admin.describeConfigs(java.util.List.of(brokerResource, topicResource)).all().get(10, TimeUnit.SECONDS)

        val brokerConfig = described.get(brokerResource)
        assertEquals(brokerConfig.get("broker.id").value(), broker.config.nodeId.toString)
        assertEquals(brokerConfig.get("broker.id").source(), ConfigEntry.ConfigSource.STATIC_BROKER_CONFIG)
        assert(!brokerConfig.entries().asScala.exists(_.isSensitive()))
        assertEquals(brokerConfig.get("log.segment.bytes").value(), (8L * 1024 * 1024).toString)

        val topicConfig = described.get(topicResource)
        assertEquals(topicConfig.get("cleanup.policy").value(), "delete")
        assertEquals(topicConfig.get("cleanup.policy").source(), ConfigEntry.ConfigSource.DEFAULT_CONFIG)
        assertEquals(topicConfig.get("min.insync.replicas").value(), "1")
      finally admin.close(Duration.ofSeconds(5))
    finally
      broker.close()
      deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, AlterConfigOp, ConfigEntry, NewTopic}
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

  test("Apache Kafka Admin changes durable per-topic lifecycle configuration") {
    val directory = Files.createTempDirectory("cascade-admin-lifecycle-e2e")
    def createBroker(): KafkaBroker = KafkaBroker(
      BrokerConfig(bindHost = "127.0.0.1", port = 0, advertisedHost = "127.0.0.1", dataDirectory = directory)
    )
    val resource = ConfigResource(ConfigResource.Type.TOPIC, "policy-events")
    var broker = createBroker()
    try
      broker.start()
      withAdmin(broker) { admin =>
        admin.createTopics(java.util.List.of(NewTopic("policy-events", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
        val changes = java.util.List.of(
          AlterConfigOp(ConfigEntry("cleanup.policy", "compact,delete"), AlterConfigOp.OpType.SET),
          AlterConfigOp(ConfigEntry("retention.ms", "3600000"), AlterConfigOp.OpType.SET),
          AlterConfigOp(ConfigEntry("retention.bytes", "1073741824"), AlterConfigOp.OpType.SET)
        )
        admin.incrementalAlterConfigs(Map(resource -> changes).asJava).all().get(10, TimeUnit.SECONDS)
        assertLifecycle(admin, resource)
      }
      broker.close()

      broker = createBroker()
      broker.start()
      withAdmin(broker)(admin => assertLifecycle(admin, resource))
    finally
      broker.close()
      deleteTree(directory)
  }

  private def withAdmin(broker: KafkaBroker)(action: Admin => Unit): Unit =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    val admin = Admin.create(properties)
    try action(admin)
    finally admin.close(Duration.ofSeconds(5))

  private def assertLifecycle(admin: Admin, resource: ConfigResource): Unit =
    val config = admin.describeConfigs(java.util.List.of(resource)).all().get(10, TimeUnit.SECONDS).get(resource)
    assertEquals(config.get("cleanup.policy").value(), "compact,delete")
    assertEquals(config.get("retention.ms").value(), "3600000")
    assertEquals(config.get("retention.bytes").value(), "1073741824")
    assertEquals(config.get("retention.ms").source(), ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG)

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

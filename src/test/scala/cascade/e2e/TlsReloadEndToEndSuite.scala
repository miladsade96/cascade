package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.security.{BrokerSecurityConfig, SecurityProtocol, SecurityTestSupport, TlsConfig}
import java.nio.file.{Files, StandardCopyOption}
import java.time.Duration
import java.util.Properties
import java.util.concurrent.{ExecutionException, TimeUnit}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.common.config.SslConfigs

final class TlsReloadEndToEndSuite extends munit.FunSuite:
  test("rotates the broker certificate without dropping established Kafka clients") {
    val root = Files.createTempDirectory("cascade-tls-key-rotation-e2e")
    val first = SecurityTestSupport.createKeyStore(root, "first.p12")
    val second = SecurityTestSupport.createKeyStore(root, "second.p12")
    val active = root.resolve("active.p12")
    Files.copy(first, active)
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = root.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.Ssl,
          tls = TlsConfig(
            keyStore = Some(active),
            keyStorePassword = Some(SecurityTestSupport.StorePassword),
            reloadIntervalMillis = 50L
          )
        )
      )
    )
    try
      broker.start()
      val established = Admin.create(clientProperties(broker.bootstrapServers, first, "established"))
      try
        established.listTopics().names().get(10, TimeUnit.SECONDS): Unit

        Files.copy(second, active, StandardCopyOption.REPLACE_EXISTING)
        eventually(Duration.ofSeconds(10)) {
          broker.metricsSnapshot.tlsReload.generation == 1L
        }
        assertEquals(broker.metricsSnapshot.tlsReload.successfulReloads, 1L)
        assert(broker.healthSnapshot.ready)

        established.describeCluster().clusterId().get(10, TimeUnit.SECONDS): Unit

        val rotated = Admin.create(clientProperties(broker.bootstrapServers, second, "rotated"))
        try
          rotated.createTopics(java.util.List.of(NewTopic("after-key-rotation", 1, 1.toShort)))
            .all().get(10, TimeUnit.SECONDS): Unit
        finally rotated.close(Duration.ofSeconds(5))

        val staleTrust = Admin.create(clientProperties(broker.bootstrapServers, first, "stale-trust"))
        try
          intercept[ExecutionException] {
            staleTrust.describeCluster().clusterId().get(5, TimeUnit.SECONDS)
          }
        finally staleTrust.close(Duration.ofSeconds(1))
      finally established.close(Duration.ofSeconds(5))
    finally
      broker.close()
      SecurityTestSupport.deleteTree(root)
  }

  private def clientProperties(bootstrap: String, trustStore: java.nio.file.Path, clientId: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId)
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "2000")
    properties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "4000")
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.toString)
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
    properties

  private def eventually(timeout: Duration)(condition: => Boolean): Unit =
    val deadline = System.nanoTime() + timeout.toNanos
    var satisfied = condition
    while !satisfied && System.nanoTime() < deadline do
      Thread.sleep(25L)
      satisfied = condition
    assert(satisfied, s"condition was not satisfied within $timeout")

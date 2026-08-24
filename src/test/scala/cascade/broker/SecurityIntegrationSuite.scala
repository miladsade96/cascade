package cascade.broker

import cascade.security.*
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SslConfigs

final class SecurityIntegrationSuite extends FunSuite:
  test("Kafka clients negotiate TLS with the broker") {
    val directory = Files.createTempDirectory("cascade-tls-integration")
    val keyStore = SecurityTestSupport.createKeyStore(directory)
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.Ssl,
          tls = TlsConfig(keyStore = Some(keyStore), keyStorePassword = Some(SecurityTestSupport.StorePassword))
        )
      )
    )
    try
      broker.start()
      val properties = Properties()
      properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
      properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStore.toString)
      properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
      properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
      val admin = Admin.create(properties)
      try assertEquals(admin.describeMetadataQuorum().quorumInfo().get(10, TimeUnit.SECONDS).leaderId(), 1)
      finally admin.close(Duration.ofSeconds(5))
    finally
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

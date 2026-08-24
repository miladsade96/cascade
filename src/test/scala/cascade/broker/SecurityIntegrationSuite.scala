package cascade.broker

import cascade.security.*
import cascade.protocol.*
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}

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

  test("advertises and negotiates Kafka SASL PLAIN framing") {
    val directory = Files.createTempDirectory("cascade-sasl-handshake")
    val credentials = directory.resolve("users.conf")
    val password = "test-password".toCharArray
    Files.writeString(credentials, s"alice=${CredentialHash.create(password, CredentialHash.MinimumIterations)}\n")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.SaslPlaintext,
          authentication = AuthenticationConfig(credentialsFile = Some(credentials))
        )
      )
    )
    try
      broker.start()
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        val request = ByteWriter()
          .writeShort(ApiKey.SaslHandshake)
          .writeShort(1)
          .writeInt(7)
          .writeNullableString(Some("security-test"))
          .writeString("PLAIN")
          .result()
        output.writeInt(request.length)
        output.write(request)
        output.flush()
        val response = new Array[Byte](input.readInt())
        input.readFully(response)
        val cursor = ByteCursor(response)
        assertEquals(cursor.readInt(), 7)
        assertEquals(cursor.readShort(), Errors.None)
        assertEquals(cursor.readArray(cursor.readString()), Vector("PLAIN"))
        cursor.ensureFullyRead()
      finally socket.close()
    finally
      java.util.Arrays.fill(password, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  test("authenticates Apache Kafka clients with framed SASL PLAIN") {
    val directory = Files.createTempDirectory("cascade-sasl-client")
    val credentials = directory.resolve("users.conf")
    val password = "kafka-client-password".toCharArray
    Files.writeString(credentials, s"alice=${CredentialHash.create(password, CredentialHash.MinimumIterations)}\n")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.SaslPlaintext,
          authentication = AuthenticationConfig(credentialsFile = Some(credentials))
        )
      )
    )
    try
      broker.start()
      val properties = Properties()
      properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
      properties.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"kafka-client-password\";"
      )
      val admin = Admin.create(properties)
      try assertEquals(admin.describeMetadataQuorum().quorumInfo().get(10, TimeUnit.SECONDS).leaderId(), 1)
      finally admin.close(Duration.ofSeconds(5))
    finally
      java.util.Arrays.fill(password, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

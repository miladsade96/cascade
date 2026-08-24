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
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.errors.TopicAuthorizationException
import org.apache.kafka.common.serialization.ByteArraySerializer

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

  test("encrypts SASL authentication and applies rotated credentials to new connections") {
    val directory = Files.createTempDirectory("cascade-sasl-ssl")
    val keyStore = SecurityTestSupport.createKeyStore(directory)
    val credentials = directory.resolve("users.conf")
    val firstPassword = "first-client-password".toCharArray
    val secondPassword = "rotated-client-password".toCharArray
    Files.writeString(credentials, s"alice=${CredentialHash.create(firstPassword, CredentialHash.MinimumIterations)}\n")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.SaslSsl,
          tls = TlsConfig(keyStore = Some(keyStore), keyStorePassword = Some(SecurityTestSupport.StorePassword)),
          authentication = AuthenticationConfig(credentialsFile = Some(credentials), reloadIntervalMillis = 0L)
        )
      )
    )
    try
      broker.start()
      assertSecureAdmin(broker, keyStore, "first-client-password")

      Files.writeString(credentials, s"alice=${CredentialHash.create(secondPassword, CredentialHash.MinimumIterations)}\n")
      assertSecureAdmin(broker, keyStore, "rotated-client-password")

      val rejected = Admin.create(secureSaslProperties(broker, keyStore, "first-client-password"))
      try intercept[java.util.concurrent.ExecutionException] {
        rejected.describeMetadataQuorum().quorumInfo().get(5, TimeUnit.SECONDS)
      }
      finally rejected.close(Duration.ofSeconds(1))
    finally
      java.util.Arrays.fill(firstPassword, '\u0000')
      java.util.Arrays.fill(secondPassword, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  test("enforces topic ACLs before records reach storage") {
    val directory = Files.createTempDirectory("cascade-topic-acls")
    val credentials = directory.resolve("users.conf")
    val acls = directory.resolve("acls.conf")
    val audit = directory.resolve("security-audit.jsonl")
    val password = "acl-client-password".toCharArray
    Files.writeString(credentials, s"alice=${CredentialHash.create(password, CredentialHash.MinimumIterations)}\n")
    Files.writeString(
      acls,
      """allow alice Describe Topic *
        |allow alice Create Topic *
        |allow alice Write Topic allowed
        |allow alice IdempotentWrite Cluster cascade
        |""".stripMargin
    )
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.SaslPlaintext,
          authentication = AuthenticationConfig(credentialsFile = Some(credentials)),
          authorization = AuthorizationConfig(aclFile = Some(acls)),
          audit = AuditConfig(path = Some(audit))
        )
      )
    )
    try
      broker.start()
      val properties = Properties()
      properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
      properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
      properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000")
      properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
      properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
      properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
      properties.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"acl-client-password\";"
      )
      val producer = KafkaProducer[Array[Byte], Array[Byte]](properties)
      try
        assertEquals(
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("allowed", "accepted".getBytes())).get(10, TimeUnit.SECONDS).offset(),
          0L
        )
        val rejected = intercept[java.util.concurrent.ExecutionException] {
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]]("blocked", "rejected".getBytes())).get(10, TimeUnit.SECONDS)
        }
        assert(rejected.getCause.isInstanceOf[TopicAuthorizationException])
        val events = Files.readString(audit)
        assert(events.contains("\"event\":\"authentication\""))
        assert(events.contains("\"decision\":\"denied\""))
        assert(events.contains("\"resource\":\"blocked\""))
      finally producer.close(Duration.ofSeconds(5))
    finally
      java.util.Arrays.fill(password, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  private def assertSecureAdmin(broker: KafkaBroker, keyStore: java.nio.file.Path, password: String): Unit =
    val admin = Admin.create(secureSaslProperties(broker, keyStore, password))
    try assertEquals(admin.describeMetadataQuorum().quorumInfo().get(10, TimeUnit.SECONDS).leaderId(), 1)
    finally admin.close(Duration.ofSeconds(5))

  private def secureSaslProperties(broker: KafkaBroker, keyStore: java.nio.file.Path, password: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
    properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "3000")
    properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "3000")
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    properties.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"$password\";"
    )
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStore.toString)
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
    properties

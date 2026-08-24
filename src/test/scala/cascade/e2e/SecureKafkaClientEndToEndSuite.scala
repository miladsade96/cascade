package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.security.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class SecureKafkaClientEndToEndSuite extends FunSuite:
  test("Kafka producer and group consumer interoperate through TLS, SASL, and ACLs") {
    val directory = Files.createTempDirectory("cascade-secure-e2e")
    val keyStore = SecurityTestSupport.createKeyStore(directory)
    val credentials = directory.resolve("users.conf")
    val acls = directory.resolve("acls.conf")
    val audit = directory.resolve("audit.jsonl")
    val password = "secure-e2e-password".toCharArray
    Files.writeString(credentials, s"alice=${CredentialHash.create(password, CredentialHash.MinimumIterations)}\n")
    Files.writeString(
      acls,
      """allow alice Create Topic secure-events
        |allow alice Describe Topic secure-events
        |allow alice Write Topic secure-events
        |allow alice Read Topic secure-events
        |allow alice Read Group secure-group
        |allow alice Describe Group secure-group
        |""".stripMargin
    )
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.SaslSsl,
          tls = TlsConfig(keyStore = Some(keyStore), keyStorePassword = Some(SecurityTestSupport.StorePassword)),
          authentication = AuthenticationConfig(credentialsFile = Some(credentials)),
          authorization = AuthorizationConfig(aclFile = Some(acls)),
          audit = AuditConfig(path = Some(audit))
        )
      )
    )
    try
      broker.start()
      val admin = Admin.create(clientProperties(broker, keyStore))
      try admin.createTopics(java.util.List.of(NewTopic("secure-events", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
      finally admin.close(Duration.ofSeconds(5))

      val producerProperties = clientProperties(broker, keyStore)
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
      producerProperties.put(ProducerConfig.ACKS_CONFIG, "all")
      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
      try
        val metadata = producer.send(
          new ProducerRecord[Array[Byte], Array[Byte]]("secure-events", "protected-payload".getBytes(StandardCharsets.UTF_8))
        ).get(10, TimeUnit.SECONDS)
        assertEquals(metadata.offset(), 0L)
      finally producer.close(Duration.ofSeconds(5))

      val consumerProperties = clientProperties(broker, keyStore)
      consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "secure-group")
      consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      consumerProperties.put("group.protocol", "classic")
      val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties)
      try
        consumer.subscribe(java.util.List.of("secure-events"))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15)
        var values = Vector.empty[String]
        while values.isEmpty && System.nanoTime() < deadline do
          values = consumer.poll(Duration.ofMillis(250)).iterator().asScala.map { record =>
            String(record.value(), StandardCharsets.UTF_8)
          }.toVector
        assertEquals(values, Vector("protected-payload"))
        consumer.commitSync()
      finally consumer.close()

      val auditEvents = Files.readString(audit)
      assert(auditEvents.contains("\"secure\":\"true\""))
      assert(auditEvents.contains("\"resource\":\"secure-events\""))
      assert(auditEvents.contains("\"resource\":\"secure-group\""))
    finally
      java.util.Arrays.fill(password, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  private def clientProperties(broker: KafkaBroker, keyStore: java.nio.file.Path): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    properties.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"alice\" password=\"secure-e2e-password\";"
    )
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStore.toString)
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
    properties

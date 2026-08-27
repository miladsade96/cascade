package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.security.*
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class ScramKafkaClientEndToEndSuite extends munit.FunSuite:
  test("Kafka clients produce and consume through SCRAM-SHA-512, TLS, ACLs, and live verifier rotation") {
    val directory = Files.createTempDirectory("cascade-scram-e2e")
    val keyStore = SecurityTestSupport.createKeyStore(directory)
    val scramCredentials = directory.resolve("scram-users.conf")
    val acls = directory.resolve("acls.conf")
    val audit = directory.resolve("audit.jsonl")
    val firstPassword = "first-scram-e2e-password".toCharArray
    val secondPassword = "rotated-scram-e2e-password".toCharArray
    writeCredential(scramCredentials, firstPassword)
    Files.writeString(
      acls,
      """allow alice Create Topic scram-events
        |allow alice Describe Topic scram-events
        |allow alice Write Topic scram-events
        |allow alice Read Topic scram-events
        |allow alice Read Group scram-group
        |allow alice Describe Group scram-group
        |allow alice Describe Cluster cascade
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
          authentication = AuthenticationConfig(
            scramCredentialsFile = Some(scramCredentials),
            mechanisms = Vector(SaslMechanism.ScramSha512),
            reloadIntervalMillis = 0L
          ),
          authorization = AuthorizationConfig(aclFile = Some(acls)),
          audit = AuditConfig(Some(audit))
        )
      )
    )
    try
      broker.start()
      val firstProperties = clientProperties(broker, keyStore, "first-scram-e2e-password")
      val admin = Admin.create(firstProperties)
      try admin.createTopics(java.util.List.of(NewTopic("scram-events", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
      finally admin.close(Duration.ofSeconds(5))

      val producerProperties = copy(firstProperties)
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
      try
        (0 until 5).foreach { index =>
          val metadata = producer.send(
            ProducerRecord[Array[Byte], Array[Byte]]("scram-events", s"scram-$index".getBytes(StandardCharsets.UTF_8))
          ).get(10, TimeUnit.SECONDS)
          assertEquals(metadata.offset(), index.toLong)
        }
      finally producer.close(Duration.ofSeconds(5))

      val consumerProperties = copy(firstProperties)
      consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "scram-group")
      consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties)
      try
        val partition = TopicPartition("scram-events", 0)
        consumer.assign(java.util.List.of(partition))
        consumer.seekToBeginning(java.util.List.of(partition))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        val values = scala.collection.mutable.ArrayBuffer.empty[String]
        while values.size < 5 && System.nanoTime() < deadline do
          consumer.poll(Duration.ofMillis(250)).iterator().asScala.foreach { record =>
            values += String(record.value(), StandardCharsets.UTF_8)
          }
        assertEquals(values.toVector, (0 until 5).map(index => s"scram-$index").toVector)
      finally consumer.close()

      writeCredential(scramCredentials, secondPassword)
      assertAdmin(broker, keyStore, "rotated-scram-e2e-password")
      val rejected = Admin.create(clientProperties(broker, keyStore, "first-scram-e2e-password"))
      try intercept[java.util.concurrent.ExecutionException] {
        rejected.describeMetadataQuorum().quorumInfo().get(5, TimeUnit.SECONDS)
      }
      finally rejected.close(Duration.ofSeconds(1))

      Files.writeString(scramCredentials, "malformed", StandardCharsets.UTF_8): Unit
      assert(!broker.healthSnapshot.ready)
      assert(broker.healthSnapshot.failedChecks.exists(_.name == "credential_policy"))
      assertAdmin(broker, keyStore, "rotated-scram-e2e-password")
      writeCredential(scramCredentials, secondPassword)
      assert(broker.healthSnapshot.ready)

      val metrics = broker.metricsSnapshot.authentication
      assert(metrics.mechanisms.find(_.mechanism == "SCRAM-SHA-512").exists(_.successes >= 4L))
      assert(metrics.mechanisms.find(_.mechanism == "SCRAM-SHA-512").exists(_.failures >= 1L))
      val events = Files.readString(audit)
      assert(events.contains("\"mechanism\":\"SCRAM-SHA-512\""))
      assert(events.contains("\"decision\":\"allowed\""))
      assert(events.contains("\"decision\":\"denied\""))
    finally
      java.util.Arrays.fill(firstPassword, '\u0000')
      java.util.Arrays.fill(secondPassword, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  private def writeCredential(path: java.nio.file.Path, password: Array[Char]): Unit =
    val line = CredentialTool.generateScramLine(
      "alice",
      password,
      SaslMechanism.ScramSha512,
      ScramCredential.MinimumIterations
    )
    Files.writeString(path, line + "\n", StandardCharsets.UTF_8): Unit

  private def assertAdmin(broker: KafkaBroker, keyStore: java.nio.file.Path, password: String): Unit =
    val admin = Admin.create(clientProperties(broker, keyStore, password))
    try assertEquals(admin.describeMetadataQuorum().quorumInfo().get(10, TimeUnit.SECONDS).leaderId(), 1)
    finally admin.close(Duration.ofSeconds(5))

  private def clientProperties(broker: KafkaBroker, keyStore: java.nio.file.Path, password: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
    properties.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"alice\" password=\"$password\";"
    )
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStore.toString)
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
    properties

  private def copy(source: Properties): Properties =
    val target = Properties()
    target.putAll(source)
    target

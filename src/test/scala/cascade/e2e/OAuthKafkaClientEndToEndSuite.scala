package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.security.*
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.{Collections, Properties}
import java.util.concurrent.TimeUnit
import javax.security.auth.callback.{Callback, UnsupportedCallbackException}
import javax.security.auth.login.AppConfigurationEntry
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler
import org.apache.kafka.common.security.oauthbearer.{OAuthBearerToken, OAuthBearerTokenCallback}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class FileOAuthBearerLoginCallbackHandler extends AuthenticateCallbackHandler:
  private var tokenFile: Path | Null = null
  private var principal = "alice"

  override def configure(
      configs: java.util.Map[String, ?],
      saslMechanism: String,
      jaasConfigEntries: java.util.List[AppConfigurationEntry]
  ): Unit =
    require(saslMechanism == "OAUTHBEARER", "test callback only supports OAUTHBEARER")
    require(!jaasConfigEntries.isEmpty, "test callback requires a JAAS entry")
    val options = jaasConfigEntries.get(0).getOptions
    tokenFile = Path.of(options.get("tokenFile").toString)
    principal = Option(options.get("principal")).map(_.toString).getOrElse("alice")

  override def handle(callbacks: Array[Callback]): Unit = callbacks.foreach {
    case callback: OAuthBearerTokenCallback =>
      val path = Option(tokenFile).getOrElse(throw IllegalStateException("callback is not configured"))
      val value = Files.readString(path, StandardCharsets.UTF_8).trim
      callback.token(FileOAuthBearerToken(value, principal))
    case callback => throw UnsupportedCallbackException(callback)
  }

  override def close(): Unit = ()

final case class FileOAuthBearerToken(tokenValue: String, user: String) extends OAuthBearerToken:
  private val start = System.currentTimeMillis()
  override def value(): String = tokenValue
  override def scope(): java.util.Set[String] = Collections.unmodifiableSet(Set("cascade.read", "cascade.write").asJava)
  override def lifetimeMs(): Long = start + TimeUnit.HOURS.toMillis(1)
  override def principalName(): String = user
  override def startTimeMs(): java.lang.Long = java.lang.Long.valueOf(start)

final class OAuthKafkaClientEndToEndSuite extends munit.FunSuite:
  test("Kafka clients use signed OAUTHBEARER JWTs through TLS, ACLs, key rotation, metrics, and readiness") {
    val directory = Files.createTempDirectory("cascade-oauth-e2e")
    val keyStore = SecurityTestSupport.createKeyStore(directory)
    val jwks = directory.resolve("jwks.json")
    val tokenFile = directory.resolve("token.jwt")
    val oldTokenFile = directory.resolve("old-token.jwt")
    val badTokenFile = directory.resolve("bad-token.jwt")
    val acls = directory.resolve("acls.conf")
    val audit = directory.resolve("audit.jsonl")
    val first = OAuthTestSupport.keyPair()
    val second = OAuthTestSupport.keyPair()
    val now = java.time.Instant.now().getEpochSecond
    val firstToken = token(first, "first", now, "cascade")
    OAuthTestSupport.writeJwks(jwks, Vector("first" -> first))
    write(tokenFile, firstToken)
    write(oldTokenFile, firstToken)
    write(badTokenFile, token(first, "first", now, "wrong-audience"))
    Files.writeString(
      acls,
      """allow Role:publisher Create Topic oauth-events
        |allow Role:publisher Describe Topic oauth-events
        |allow Role:publisher Write Topic oauth-events
        |allow Role:consumer Read Topic oauth-events
        |allow Role:consumer Read Group oauth-group
        |allow Role:consumer Describe Group oauth-group
        |allow alice Describe Cluster cascade
        |""".stripMargin,
      StandardCharsets.UTF_8
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
            mechanisms = Vector(SaslMechanism.OAuthBearer),
            oauth = OAuthConfig(
              jwksUri = Some(jwks.toUri),
              issuer = Some("https://issuer.example"),
              audience = Some("cascade"),
              roleClaim = Some("groups"),
              roleMappings = Map("engineering" -> "publisher", "analysts" -> "consumer"),
              requiredScopes = Set("cascade.write"),
              jwksRefreshMillis = 0L
            )
          ),
          authorization = AuthorizationConfig(aclFile = Some(acls)),
          audit = AuditConfig(Some(audit))
        )
      )
    )
    try
      broker.start()
      val firstProperties = clientProperties(broker, keyStore, tokenFile)
      val admin = Admin.create(firstProperties)
      try admin.createTopics(java.util.List.of(NewTopic("oauth-events", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
      finally admin.close(Duration.ofSeconds(5))

      val producerProperties = copy(firstProperties)
      producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties)
      try
        (0 until 5).foreach { index =>
          val metadata = producer.send(
            ProducerRecord[Array[Byte], Array[Byte]]("oauth-events", s"oauth-$index".getBytes(StandardCharsets.UTF_8))
          ).get(10, TimeUnit.SECONDS)
          assertEquals(metadata.offset(), index.toLong)
        }
      finally producer.close(Duration.ofSeconds(5))

      val consumerProperties = copy(firstProperties)
      consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
      consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "oauth-group")
      consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties)
      try
        val partition = TopicPartition("oauth-events", 0)
        consumer.assign(java.util.List.of(partition))
        consumer.seekToBeginning(java.util.List.of(partition))
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        val values = scala.collection.mutable.ArrayBuffer.empty[String]
        while values.size < 5 && System.nanoTime() < deadline do
          consumer.poll(Duration.ofMillis(250)).iterator().asScala.foreach { record =>
            values += String(record.value(), StandardCharsets.UTF_8)
          }
        assertEquals(values.toVector, (0 until 5).map(index => s"oauth-$index").toVector)
      finally consumer.close()

      OAuthTestSupport.writeJwks(jwks, Vector("second" -> second))
      write(tokenFile, token(second, "second", now, "cascade"))
      assertAdmin(broker, keyStore, tokenFile)
      assertRejected(broker, keyStore, oldTokenFile)
      assertRejected(broker, keyStore, badTokenFile)

      Files.writeString(jwks, "malformed", StandardCharsets.UTF_8): Unit
      assert(!broker.healthSnapshot.ready)
      assert(broker.healthSnapshot.failedChecks.exists(_.name == "credential_policy"))
      assertAdmin(broker, keyStore, tokenFile)
      OAuthTestSupport.writeJwks(jwks, Vector("second" -> second))
      assert(broker.healthSnapshot.ready)

      val metrics = broker.metricsSnapshot.authentication
      assert(metrics.mechanisms.find(_.mechanism == "OAUTHBEARER").exists(_.successes >= 4L))
      assert(metrics.mechanisms.find(_.mechanism == "OAUTHBEARER").exists(_.failures >= 2L))
      val events = Files.readString(audit)
      assert(events.contains("\"mechanism\":\"OAUTHBEARER\""))
      assert(events.contains("\"decision\":\"allowed\""))
      assert(events.contains("\"decision\":\"denied\""))
      assert(!events.contains(firstToken))
    finally
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  private def token(pair: java.security.KeyPair, keyId: String, now: Long, audience: String): String =
    val claims = OAuthTestSupport.claims(
      "https://issuer.example",
      s"\"$audience\"",
      "alice",
      now - 10,
      now + 600,
      extra = "\"groups\":[\"engineering\",\"analysts\",\"untrusted-admin\"]"
    )
    OAuthTestSupport.token(pair.getPrivate, keyId, claims)

  private def write(path: Path, value: String): Unit =
    Files.writeString(path, value, StandardCharsets.UTF_8): Unit

  private def assertAdmin(broker: KafkaBroker, keyStore: Path, tokenFile: Path): Unit =
    val admin = Admin.create(clientProperties(broker, keyStore, tokenFile))
    try assertEquals(admin.describeMetadataQuorum().quorumInfo().get(10, TimeUnit.SECONDS).leaderId(), 1)
    finally admin.close(Duration.ofSeconds(5))

  private def assertRejected(broker: KafkaBroker, keyStore: Path, tokenFile: Path): Unit =
    val admin = Admin.create(clientProperties(broker, keyStore, tokenFile))
    try intercept[java.util.concurrent.ExecutionException] {
      admin.describeMetadataQuorum().quorumInfo().get(5, TimeUnit.SECONDS)
    }: Unit
    finally admin.close(Duration.ofSeconds(1))

  private def clientProperties(broker: KafkaBroker, keyStore: Path, tokenFile: Path): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
    properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER")
    properties.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required tokenFile=\"${tokenFile.toString.replace('\\', '/')}\" principal=\"alice\";"
    )
    properties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, classOf[FileOAuthBearerLoginCallbackHandler].getName)
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, keyStore.toString)
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
    properties

  private def copy(source: Properties): Properties =
    val target = Properties()
    target.putAll(source)
    target

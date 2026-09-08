package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.security.*
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter, AclOperation as KafkaAclOperation, AclPermissionType, AccessControlEntry}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.{GroupAuthorizationException, TopicAuthorizationException}
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType as KafkaResourceType}
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.TopicPartition
import scala.jdk.CollectionConverters.*

final class AclAdminEndToEndSuite extends FunSuite:
  test("Kafka Admin creates, describes, persists, activates, and deletes ACLs") {
    val directory = Files.createTempDirectory("cascade-acl-admin-e2e")
    val credentials = directory.resolve("users.conf")
    val acls = directory.resolve("acls.conf")
    val alicePassword = "acl-admin-password".toCharArray
    val bobPassword = "acl-client-password".toCharArray
    Files.writeString(
      credentials,
      s"alice=${CredentialHash.create(alicePassword, CredentialHash.MinimumIterations)}\n" +
        s"bob=${CredentialHash.create(bobPassword, CredentialHash.MinimumIterations)}\n"
    )
    Files.writeString(acls, "")
    def createBroker(): KafkaBroker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory.resolve("data"),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.SaslPlaintext,
          authentication = AuthenticationConfig(credentialsFile = Some(credentials)),
          authorization = AuthorizationConfig(aclFile = Some(acls), superUsers = Set("alice"))
        )
      )
    )
    val describe = binding("orders-live", PatternType.LITERAL, KafkaAclOperation.DESCRIBE)
    val writePrefix = binding("orders-", PatternType.PREFIXED, KafkaAclOperation.WRITE)
    val readPrefix = binding("orders-", PatternType.PREFIXED, KafkaAclOperation.READ)
    var broker = createBroker()
    try
      broker.start()
      withAdmin(broker) { admin =>
        admin.createTopics(java.util.List.of(NewTopic("orders-live", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
        admin.createAcls(java.util.List.of(describe, writePrefix, readPrefix)).all().get(10, TimeUnit.SECONDS)
        assertEquals(admin.describeAcls(AclBindingFilter.ANY).values().get(10, TimeUnit.SECONDS).asScala.toSet, Set(describe, writePrefix, readPrefix))
      }
      broker.close()

      broker = createBroker()
      broker.start()
      withAdmin(broker) { admin =>
        assertEquals(admin.describeAcls(AclBindingFilter.ANY).values().get(10, TimeUnit.SECONDS).asScala.toSet, Set(describe, writePrefix, readPrefix))
      }
      produceAsBob(broker, "allowed")
      withAdmin(broker) { admin =>
        assertEquals(admin.deleteAcls(java.util.List.of(writePrefix.toFilter)).all().get(10, TimeUnit.SECONDS).asScala.toSet, Set(writePrefix))
        assertEquals(admin.describeAcls(AclBindingFilter.ANY).values().get(10, TimeUnit.SECONDS).asScala.toSet, Set(describe, readPrefix))
      }
      val denied = intercept[java.util.concurrent.ExecutionException](produceAsBob(broker, "denied"))
      assert(denied.getCause.isInstanceOf[TopicAuthorizationException])
    finally
      java.util.Arrays.fill(alicePassword, '\u0000')
      java.util.Arrays.fill(bobPassword, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  test("group administration enforces per-group describe and delete ACLs") {
    val directory = Files.createTempDirectory("cascade-group-acl-e2e")
    val credentials = directory.resolve("users.conf")
    val acls = directory.resolve("acls.conf")
    val alicePassword = "group-admin-password".toCharArray
    val bobPassword = "group-reader-password".toCharArray
    Files.writeString(
      credentials,
      s"alice=${CredentialHash.create(alicePassword, CredentialHash.MinimumIterations)}\n" +
        s"bob=${CredentialHash.create(bobPassword, CredentialHash.MinimumIterations)}\n"
    )
    Files.writeString(
      acls,
      """allow bob Describe Group visible-group
        |allow bob Delete Group visible-group
        |allow bob Describe Group describe-only-group
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
          authorization = AuthorizationConfig(aclFile = Some(acls), superUsers = Set("alice"))
        )
      )
    )
    try
      broker.start()
      val admin = Admin.create(clientProperties(broker, "alice", "group-admin-password"))
      try
        admin.createTopics(java.util.List.of(NewTopic("group-admin-events", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
        Vector("visible-group", "describe-only-group", "hidden-group").zipWithIndex.foreach { (groupId, index) =>
          commitGroupOffset(broker, groupId, index.toLong, "alice", "group-admin-password")
        }
      finally admin.close(Duration.ofSeconds(5))

      val bobAdmin = Admin.create(clientProperties(broker, "bob", "group-reader-password"))
      try
        val visible = bobAdmin.listConsumerGroups().all().get(10, TimeUnit.SECONDS).asScala.map(_.groupId()).toSet
        assertEquals(visible, Set("visible-group", "describe-only-group"))

        val described = bobAdmin.describeConsumerGroups(java.util.List.of("visible-group")).all().get(10, TimeUnit.SECONDS)
        assertEquals(described.get("visible-group").groupId(), "visible-group")
        val describeDenied = intercept[java.util.concurrent.ExecutionException] {
          bobAdmin.describeConsumerGroups(java.util.List.of("hidden-group")).all().get(10, TimeUnit.SECONDS)
        }
        assert(describeDenied.getCause.isInstanceOf[GroupAuthorizationException])

        bobAdmin.deleteConsumerGroups(java.util.List.of("visible-group")).all().get(10, TimeUnit.SECONDS)
        val deleteDenied = intercept[java.util.concurrent.ExecutionException] {
          bobAdmin.deleteConsumerGroups(java.util.List.of("describe-only-group")).all().get(10, TimeUnit.SECONDS)
        }
        assert(deleteDenied.getCause.isInstanceOf[GroupAuthorizationException])
      finally bobAdmin.close(Duration.ofSeconds(5))
    finally
      java.util.Arrays.fill(alicePassword, '\u0000')
      java.util.Arrays.fill(bobPassword, '\u0000')
      broker.close()
      SecurityTestSupport.deleteTree(directory)
  }

  private def binding(name: String, patternType: PatternType, operation: KafkaAclOperation): AclBinding =
    AclBinding(
      ResourcePattern(KafkaResourceType.TOPIC, name, patternType),
      AccessControlEntry("User:bob", "*", operation, AclPermissionType.ALLOW)
    )

  private def withAdmin(broker: KafkaBroker)(action: Admin => Unit): Unit =
    val admin = Admin.create(clientProperties(broker, "alice", "acl-admin-password"))
    try action(admin)
    finally admin.close(Duration.ofSeconds(5))

  private def produceAsBob(broker: KafkaBroker, value: String): Unit =
    val properties = clientProperties(broker, "bob", "acl-client-password")
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    properties.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "10000")
    val producer = KafkaProducer[Array[Byte], Array[Byte]](properties)
    try producer.send(ProducerRecord[Array[Byte], Array[Byte]]("orders-live", value.getBytes())).get(10, TimeUnit.SECONDS): Unit
    finally producer.close(Duration.ofSeconds(5))

  private def commitGroupOffset(
      broker: KafkaBroker,
      groupId: String,
      offset: Long,
      user: String,
      password: String
  ): Unit =
    val properties = clientProperties(broker, user, password)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](properties)
    val partition = TopicPartition("group-admin-events", 0)
    try
      consumer.assign(java.util.List.of(partition))
      consumer.commitSync(Map(partition -> OffsetAndMetadata(offset)).asJava)
    finally consumer.close()

  private def clientProperties(broker: KafkaBroker, user: String, password: String): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT")
    properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "5000")
    properties.put(CommonClientConfigs.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
    properties.put(SaslConfigs.SASL_MECHANISM, "PLAIN")
    properties.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$user\" password=\"$password\";"
    )
    properties

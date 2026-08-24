package cascade.e2e

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.cluster.ClusterNode
import cascade.security.*
import cascade.storage.FlushPolicy
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

final class SecurePeerClusterEndToEndSuite extends munit.FunSuite:
  test("three brokers replicate and fail over while every peer request uses authenticated TLS") {
    val root = Files.createTempDirectory("cascade-secure-peer-cluster")
    val ports = freePorts(3)
    val nodes = ports.zipWithIndex.map { case (port, index) => ClusterNode(index + 1, "localhost", port) }
    val material = SecurityTestSupport.createMutualTlsMaterial(root, nodes.map(_.id))
    val identities = root.resolve("peer-identities.conf")
    Files.writeString(
      identities,
      material.principals.toVector.sortBy(_._1).map { case (nodeId, principal) => s"$nodeId $principal" }.mkString("", "\n", "\n"),
      StandardCharsets.UTF_8
    ): Unit
    val brokers = nodes.map { node =>
      val tls = TlsConfig(
        keyStore = Some(material.keyStores(node.id)),
        keyStorePassword = Some(SecurityTestSupport.StorePassword),
        trustStore = Some(material.trustStore),
        trustStorePassword = Some(SecurityTestSupport.StorePassword),
        clientAuth = TlsClientAuth.Requested
      )
      KafkaBroker(
        BrokerConfig(
          bindHost = "127.0.0.1",
          port = node.port,
          advertisedHost = node.host,
          advertisedPort = Some(node.port),
          dataDirectory = root.resolve(s"data-${node.id}"),
          nodeId = node.id,
          clusterNodes = nodes,
          controllerId = 1,
          defaultReplicationFactor = 3,
          minInSyncReplicas = 2,
          peerTimeoutMillis = 3000,
          flushPolicy = FlushPolicy.Sync,
          security = BrokerSecurityConfig(
            protocol = SecurityProtocol.Ssl,
            tls = tls,
            peer = PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(identities), 100L)
          )
        )
      )
    }
    val bootstrap = nodes.map(node => s"${node.host}:${node.port}").mkString(",")
    try
      brokers.foreach(_.start())
      val admin = Admin.create(clientProperties(bootstrap, material.trustStore))
      try admin.createTopics(java.util.List.of(NewTopic("secure-replicated", 1, 3.toShort))).all().get(20, TimeUnit.SECONDS)
      finally admin.close(Duration.ofSeconds(5))

      val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap, material.trustStore))
      try
        (0 until 20).foreach { index =>
          val metadata = producer.send(
            ProducerRecord[Array[Byte], Array[Byte]](
              "secure-replicated",
              0,
              null,
              s"secure-$index".getBytes(StandardCharsets.UTF_8)
            )
          ).get(15, TimeUnit.SECONDS)
          assertEquals(metadata.offset(), index.toLong)
        }
      finally producer.close(Duration.ofSeconds(5))

      brokers.head.close()
      val failoverProducer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(bootstrap, material.trustStore))
      try
        val metadata = failoverProducer.send(
          ProducerRecord[Array[Byte], Array[Byte]](
            "secure-replicated",
            0,
            null,
            "secure-20".getBytes(StandardCharsets.UTF_8)
          )
        ).get(30, TimeUnit.SECONDS)
        assertEquals(metadata.offset(), 20L)
      finally failoverProducer.close(Duration.ofSeconds(5))

      val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(bootstrap, material.trustStore))
      try
        val partition = TopicPartition("secure-replicated", 0)
        consumer.assign(java.util.List.of(partition))
        consumer.seekToBeginning(java.util.List.of(partition))
        val deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos
        val values = scala.collection.mutable.ArrayBuffer.empty[String]
        while values.size < 21 && System.nanoTime() < deadline do
          consumer.poll(Duration.ofMillis(250)).iterator().asScala.foreach { record =>
            values += String(record.value(), StandardCharsets.UTF_8)
          }
        assertEquals(values.toVector, (0 until 21).map(index => s"secure-$index").toVector)
      finally consumer.close()

      brokers.tail.foreach { broker =>
        assert(broker.metricsSnapshot.peerSecurity.tlsAuthenticated > 0L)
        assertEquals(broker.metricsSnapshot.peerSecurity.rejected, 0L)
      }
    finally
      brokers.foreach(_.close())
      SecurityTestSupport.deleteTree(root)
  }

  private def clientProperties(bootstrap: String, trustStore: java.nio.file.Path): Properties =
    val properties = Properties()
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    properties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL")
    properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.toString)
    properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, SecurityTestSupport.StorePassword)
    properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, "PKCS12")
    properties.put(CommonClientConfigs.REQUEST_TIMEOUT_MS_CONFIG, "10000")
    properties

  private def producerProperties(bootstrap: String, trustStore: java.nio.file.Path): Properties =
    val properties = clientProperties(bootstrap, trustStore)
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
    properties.put(ProducerConfig.ACKS_CONFIG, "all")
    properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false")
    properties

  private def consumerProperties(bootstrap: String, trustStore: java.nio.file.Path): Properties =
    val properties = clientProperties(bootstrap, trustStore)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getName)
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "secure-peer-verifier")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    properties

  private def freePorts(count: Int): Vector[Int] =
    val sockets = Vector.fill(count)(ServerSocket(0))
    try sockets.map(_.getLocalPort)
    finally sockets.foreach(_.close())

package cascade.broker

import cascade.TestRecordBatch
import cascade.protocol.*
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.file.Files
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig}
import scala.jdk.CollectionConverters.*

final class BrokerIntegrationSuite extends FunSuite:
  test("close is safe before start and permanently closes the broker") {
    val directory = Files.createTempDirectory("cascade-broker-lifecycle")
    val broker = KafkaBroker(
      BrokerConfig(bindHost = "127.0.0.1", port = 0, advertisedHost = "127.0.0.1", dataDirectory = directory)
    )
    try
      broker.close()
      broker.close()
      intercept[IllegalStateException](broker.start())
    finally
      broker.close()
      deleteTree(directory)
  }

  test("serves discovery, auto-creation, produce, and fetch over persistent Kafka TCP framing") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))

        val versions = request(output, input, apiVersionsRequest(correlationId = 1))
        assertEquals(versions.readInt(), 1)
        assertEquals(versions.readShort(), Errors.None)
        val apis = versions.readCompactArray {
          val api = ApiVersion(versions.readShort(), versions.readShort(), versions.readShort())
          versions.skipTaggedFields()
          api
        }
        assert(apis.exists(_.apiKey == ApiKey.Produce))
        assert(apis.exists(api => api.apiKey == ApiKey.AddRaftVoter && api.maxVersion == 1))
        assert(apis.exists(api => api.apiKey == ApiKey.RemoveRaftVoter && api.maxVersion == 0))
        versions.readInt()
        versions.skipTaggedFields()
        versions.ensureFullyRead()

        val metadata = request(output, input, metadataRequest("events", correlationId = 2))
        assertEquals(metadata.readInt(), 2)
        assertEquals(metadata.readInt(), 0)
        assertEquals(metadata.readInt(), 1)
        assertEquals(metadata.readInt(), 1)
        assertEquals(metadata.readString(), "127.0.0.1")
        assertEquals(metadata.readInt(), broker.boundPort)

        val produced = request(output, input, produceRequest("events", TestRecordBatch.single(), correlationId = 3))
        assertEquals(produced.readInt(), 3)
        assertEquals(produced.readInt(), 1)
        assertEquals(produced.readString(), "events")
        assertEquals(produced.readInt(), 1)
        assertEquals(produced.readInt(), 0)
        assertEquals(produced.readShort(), Errors.None)
        assertEquals(produced.readLong(), 0L)
        assertEquals(broker.flushStatistics.forces, 0L)
        assert(broker.flushStatistics.pendingBytes > 0L)

        val fetched = request(output, input, fetchRequest("events", correlationId = 4))
        assertEquals(fetched.readInt(), 4)
        assertEquals(fetched.readInt(), 0)
        assertEquals(fetched.readInt(), 1)
        assertEquals(fetched.readString(), "events")
        assertEquals(fetched.readInt(), 1)
        assertEquals(fetched.readInt(), 0)
        assertEquals(fetched.readShort(), Errors.None)
        assertEquals(fetched.readLong(), 1L)
        assertEquals(fetched.readLong(), 1L)
        assertEquals(fetched.readLong(), 0L)
        assertEquals(fetched.readInt(), -1)
        val records = fetched.readNullableBytes().getOrElse(fail("missing record set"))
        assertEquals(cascade.storage.RecordBatch.baseOffset(records), 0L)
      finally socket.close()
    }
  }

  test("idempotent producer retries return the original offset and reject sequence gaps") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("idempotent", correlationId = 1))

        val initialized = request(output, input, initProducerIdRequest(correlationId = 2))
        assertEquals(initialized.readInt(), 2)
        assertEquals(initialized.readInt(), 0)
        assertEquals(initialized.readShort(), Errors.None)
        val producerId = initialized.readLong()
        val producerEpoch = initialized.readShort()
        initialized.ensureFullyRead()

        val batch = TestRecordBatch.producer(producerId, producerEpoch, baseSequence = 0)
        val first = request(output, input, produceRequest("idempotent", batch, correlationId = 3))
        assertProduceResult(first, 3, Errors.None, 0L)
        val duplicate = request(output, input, produceRequest("idempotent", batch, correlationId = 4))
        assertProduceResult(duplicate, 4, Errors.None, 0L)

        val gap = TestRecordBatch.producer(producerId, producerEpoch, baseSequence = 2)
        val rejected = request(output, input, produceRequest("idempotent", gap, correlationId = 5))
        assertProduceResult(rejected, 5, Errors.OutOfOrderSequenceNumber, -1L)

        val fetched = request(output, input, fetchRequest("idempotent", correlationId = 6))
        assertEquals(fetched.readInt(), 6)
        fetched.readInt()
        fetched.readInt()
        fetched.readString()
        fetched.readInt()
        fetched.readInt()
        assertEquals(fetched.readShort(), Errors.None)
        assertEquals(fetched.readLong(), 1L)
        assertEquals(fetched.readLong(), 1L)
        fetched.readLong()
        fetched.readInt()
        assertEquals(fetched.readNullableBytes().map(_.length), Some(batch.length))
      finally socket.close()
    }
  }

  test("decodes Kafka flexible voter administration requests and responses") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        val add = requestHeader(ApiKey.AddRaftVoter, 1, 41, flexible = true)
          .writeCompactNullableString(Some("cascade-cluster"))
          .writeInt(5000)
          .writeInt(2)
          .writeUuid(10L, 20L)
        add.writeCompactArray(Vector(("CONTROLLER", "127.0.0.1", 9093))) { case (name, host, port) =>
          add.writeCompactString(name).writeCompactString(host).writeShort(port).writeEmptyTaggedFields(): Unit
        }
        add.writeBoolean(true).writeEmptyTaggedFields()

        val added = request(output, input, add.result())
        assertEquals(added.readInt(), 41)
        added.skipTaggedFields()
        assertEquals(added.readInt(), 0)
        assertEquals(added.readShort(), Errors.InvalidRequest)
        assert(added.readCompactNullableString().exists(_.contains("cluster mode")))
        added.skipTaggedFields()
        added.ensureFullyRead()

        val remove = requestHeader(ApiKey.RemoveRaftVoter, 0, 42, flexible = true)
          .writeCompactNullableString(Some("wrong-cluster"))
          .writeInt(1)
          .writeUuid(10L, 20L)
          .writeEmptyTaggedFields()
        val removed = request(output, input, remove.result())
        assertEquals(removed.readInt(), 42)
        removed.skipTaggedFields()
        assertEquals(removed.readInt(), 0)
        assertEquals(removed.readShort(), Errors.InconsistentClusterId)
        assert(removed.readCompactNullableString().nonEmpty)
        removed.skipTaggedFields()
        removed.ensureFullyRead()
      finally socket.close()
    }
  }

  test("Kafka Admin describes Cascade's metadata quorum") {
    withBroker { broker =>
      val properties = Properties()
      properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
      val admin = Admin.create(properties)
      try
        val quorum = admin.describeMetadataQuorum().quorumInfo().get(10, TimeUnit.SECONDS)
        assertEquals(quorum.leaderId(), 1)
        assertEquals(quorum.voters().asScala.map(_.replicaId()).toVector, Vector(1))
        assertEquals(quorum.observers().size(), 0)
        assertEquals(quorum.nodes().get(1).endpoints().asScala.head.host(), "127.0.0.1")
      finally admin.close(Duration.ofSeconds(5))
    }
  }

  test("describes non-sensitive broker and topic configuration with Kafka protocol v2") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("configured", correlationId = 50))
        val writer = requestHeader(ApiKey.DescribeConfigs, 2, 51)
        writer.writeArray(Vector((4, broker.config.nodeId.toString), (2, "configured"))) { case (resourceType, name) =>
          writer.writeByte(resourceType).writeString(name).writeNullableArray(None)(_ => ()): Unit
        }
        writer.writeBoolean(false)

        val response = request(output, input, writer.result())
        assertEquals(response.readInt(), 51)
        assertEquals(response.readInt(), 0)
        val resources = response.readArray {
          val error = response.readShort()
          response.readNullableString()
          val resourceType = response.readByte()
          val name = response.readString()
          val values = response.readArray {
            val key = response.readString()
            val value = response.readNullableString()
            val readOnly = response.readBoolean()
            val source = response.readByte()
            val sensitive = response.readBoolean()
            response.readArray {
              response.readString()
              response.readNullableString()
              response.readByte()
            }
            (key, value, readOnly, source, sensitive)
          }
          (error, resourceType, name, values)
        }
        response.ensureFullyRead()
        assertEquals(resources.map(_._1), Vector(Errors.None, Errors.None))
        assert(resources.head._4.exists(value => value._1 == "broker.id" && value._2.contains("1") && value._4 == 4.toByte))
        assert(resources(1)._4.exists(value => value._1 == "cleanup.policy" && value._2.contains("delete") && value._4 == 5.toByte))
        assert(resources.flatMap(_._4).forall(value => value._3 && !value._5))
      finally socket.close()
    }
  }

  test("idempotent sequence state recovers from partition logs after broker restart") {
    val directory = Files.createTempDirectory("cascade-idempotent-recovery")
    var producerId = -1L
    var producerEpoch = -1.toShort
    try
      val firstBroker = brokerFor(directory)
      try
        firstBroker.start()
        val socket = Socket("127.0.0.1", firstBroker.boundPort)
        try
          val input = DataInputStream(BufferedInputStream(socket.getInputStream))
          val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
          request(output, input, metadataRequest("recoverable-idempotent", correlationId = 1))
          val initialized = request(output, input, initProducerIdRequest(correlationId = 2))
          initialized.readInt()
          initialized.readInt()
          assertEquals(initialized.readShort(), Errors.None)
          producerId = initialized.readLong()
          producerEpoch = initialized.readShort()
          initialized.ensureFullyRead()

          val batch = TestRecordBatch.producer(producerId, producerEpoch, baseSequence = 0)
          assertProduceResult(
            request(output, input, produceRequest("recoverable-idempotent", batch, correlationId = 3)),
            3,
            Errors.None,
            0L,
            "recoverable-idempotent"
          )
        finally socket.close()
      finally firstBroker.close()

      val secondBroker = brokerFor(directory)
      try
        secondBroker.start()
        val socket = Socket("127.0.0.1", secondBroker.boundPort)
        try
          val input = DataInputStream(BufferedInputStream(socket.getInputStream))
          val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
          val duplicate = TestRecordBatch.producer(producerId, producerEpoch, baseSequence = 0)
          assertProduceResult(
            request(output, input, produceRequest("recoverable-idempotent", duplicate, correlationId = 4)),
            4,
            Errors.None,
            0L,
            "recoverable-idempotent"
          )

          val next = TestRecordBatch.producer(producerId, producerEpoch, baseSequence = 1)
          assertProduceResult(
            request(output, input, produceRequest("recoverable-idempotent", next, correlationId = 5)),
            5,
            Errors.None,
            1L,
            "recoverable-idempotent"
          )

          val fetched = request(output, input, fetchRequest("recoverable-idempotent", correlationId = 6))
          fetched.readInt()
          fetched.readInt()
          fetched.readInt()
          fetched.readString()
          fetched.readInt()
          fetched.readInt()
          assertEquals(fetched.readShort(), Errors.None)
          assertEquals(fetched.readLong(), 2L)
          assertEquals(fetched.readLong(), 2L)
          fetched.readLong()
          fetched.readInt()
          assertEquals(fetched.readNullableBytes().map(_.length), Some(duplicate.length + next.length))
        finally socket.close()
      finally secondBroker.close()
    finally deleteTree(directory)
  }

  private def apiVersionsRequest(correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.ApiVersions, 4, correlationId, flexible = true)
    writer.writeCompactString("cascade-test").writeCompactString("1.0").writeEmptyTaggedFields().result()

  private def metadataRequest(topic: String, correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.Metadata, 4, correlationId)
    writer.writeArray(Vector(topic))(writer.writeString).writeBoolean(true).result()

  private def produceRequest(topic: String, records: Array[Byte], correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.Produce, 3, correlationId)
    writer.writeNullableString(None).writeShort(1).writeInt(30000)
    writer.writeArray(Vector(topic)) { name =>
      writer.writeString(name)
      writer.writeArray(Vector(0)) { partition =>
        writer.writeInt(partition).writeNullableBytes(Some(records)): Unit
      }
    }
    writer.result()

  private def initProducerIdRequest(correlationId: Int): Array[Byte] =
    requestHeader(ApiKey.InitProducerId, 1, correlationId)
      .writeNullableString(None)
      .writeInt(60_000)
      .result()

  private def assertProduceResult(
      cursor: ByteCursor,
      correlationId: Int,
      expectedError: Short,
      expectedOffset: Long,
      expectedTopic: String = "idempotent"
  ): Unit =
    assertEquals(cursor.readInt(), correlationId)
    assertEquals(cursor.readInt(), 1)
    assertEquals(cursor.readString(), expectedTopic)
    assertEquals(cursor.readInt(), 1)
    assertEquals(cursor.readInt(), 0)
    assertEquals(cursor.readShort(), expectedError)
    assertEquals(cursor.readLong(), expectedOffset)
    cursor.readLong()
    assertEquals(cursor.readInt(), 0)
    cursor.ensureFullyRead()

  private def fetchRequest(topic: String, correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.Fetch, 6, correlationId)
    writer.writeInt(-1).writeInt(100).writeInt(1).writeInt(1024 * 1024).writeByte(0)
    writer.writeArray(Vector(topic)) { name =>
      writer.writeString(name)
      writer.writeArray(Vector(0)) { partition =>
        writer.writeInt(partition).writeLong(0L).writeLong(0L).writeInt(1024 * 1024): Unit
      }
    }
    writer.result()

  private def requestHeader(
      apiKey: Short,
      version: Short,
      correlationId: Int,
      flexible: Boolean = false
  ): ByteWriter =
    val writer = ByteWriter()
    writer.writeShort(apiKey).writeShort(version).writeInt(correlationId)
    if flexible then writer.writeNullableString(Some("integration")).writeEmptyTaggedFields()
    else writer.writeNullableString(Some("integration"))
    writer

  private def request(output: DataOutputStream, input: DataInputStream, payload: Array[Byte]): ByteCursor =
    output.writeInt(payload.length)
    output.write(payload)
    output.flush()
    val response = new Array[Byte](input.readInt())
    input.readFully(response)
    ByteCursor(response)

  private def withBroker(test: KafkaBroker => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-broker-integration")
    val broker = brokerFor(directory)
    try
      broker.start()
      test(broker)
    finally
      broker.close()
      deleteTree(directory)

  private def brokerFor(directory: java.nio.file.Path): KafkaBroker =
    KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory,
        segmentBytes = 1024 * 1024,
        flushIntervalMillis = 60_000,
        flushBytes = Long.MaxValue
      )
    )

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

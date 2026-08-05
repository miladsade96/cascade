package cascade.broker

import cascade.TestRecordBatch
import cascade.protocol.*
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.Socket
import java.nio.file.Files
import munit.FunSuite
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
    val broker = KafkaBroker(
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
    try
      broker.start()
      test(broker)
    finally
      broker.close()
      deleteTree(directory)

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

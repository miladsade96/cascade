package cascade.broker

import cascade.TestRecordBatch
import cascade.protocol.*
import cascade.security.ResourceLimits
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
  test("serves the flexible ConsumerGroupHeartbeat protocol with server-side assignment") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("modern-events", 70))
        val join = requestHeader(ApiKey.ConsumerGroupHeartbeat, 0, 71, flexible = true)
          .writeCompactString("modern-group")
          .writeCompactString("")
          .writeInt(0)
          .writeCompactNullableString(None)
          .writeCompactNullableString(None)
          .writeInt(30_000)
        join.writeCompactNullableArray(Some(Vector("modern-events")))(join.writeCompactString)
        join.writeCompactNullableString(None)
        join.writeCompactNullableArray(Some(Vector.empty[Unit]))(_ => ())
        join.writeEmptyTaggedFields()

        val response = request(output, input, join.result())
        assertEquals(response.readInt(), 71)
        response.skipTaggedFields()
        assertEquals(response.readInt(), 0)
        assertEquals(response.readShort(), Errors.None)
        assertEquals(response.readCompactNullableString(), None)
        assert(response.readCompactNullableString().exists(_.nonEmpty))
        assertEquals(response.readInt(), 1)
        assertEquals(response.readInt(), 5000)
        assertEquals(response.readByte(), 1.toByte)
        val assignment = response.readCompactArray {
          val id = response.readUuid()
          val partitions = response.readCompactArray(response.readInt())
          response.skipTaggedFields()
          (id, partitions)
        }
        assertEquals(assignment.map(_._2), Vector(Vector(0)))
        response.skipTaggedFields()
        response.skipTaggedFields()
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("serves ConsumerGroupHeartbeat v1 with regex subscriptions and client member IDs") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("regex-events", 72))
        val join = requestHeader(ApiKey.ConsumerGroupHeartbeat, 1, 73, flexible = true)
          .writeCompactString("regex-group")
          .writeCompactString("client-member")
          .writeInt(0)
          .writeCompactNullableString(None)
          .writeCompactNullableString(None)
          .writeInt(30_000)
        join.writeCompactNullableArray(None)(join.writeCompactString)
        join.writeCompactNullableString(Some("regex-.*"))
        join.writeCompactNullableString(Some("uniform"))
        join.writeCompactNullableArray(Some(Vector.empty[Unit]))(_ => ())
        join.writeEmptyTaggedFields()

        val response = request(output, input, join.result())
        assertEquals(response.readInt(), 73)
        response.skipTaggedFields()
        assertEquals(response.readInt(), 0)
        assertEquals(response.readShort(), Errors.None)
        assertEquals(response.readCompactNullableString(), None)
        assertEquals(response.readCompactNullableString(), Some("client-member"))
        assertEquals(response.readInt(), 1)
        assertEquals(response.readInt(), 5000)
        assertEquals(response.readByte(), 1.toByte)
        val assignment = response.readCompactArray {
          response.readUuid()
          val partitions = response.readCompactArray(response.readInt())
          response.skipTaggedFields()
          partitions
        }
        assertEquals(assignment, Vector(Vector(0)))
        response.skipTaggedFields()
        response.skipTaggedFields()
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("describes modern consumer groups with ConsumerGroupDescribe v1") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("described-events", 74))
        val join = requestHeader(ApiKey.ConsumerGroupHeartbeat, 1, 75, flexible = true)
          .writeCompactString("described-group")
          .writeCompactString("described-member")
          .writeInt(0)
          .writeCompactNullableString(Some("instance-a"))
          .writeCompactNullableString(Some("rack-a"))
          .writeInt(30_000)
        join.writeCompactNullableArray(Some(Vector("described-events")))(join.writeCompactString)
        join.writeCompactNullableString(None)
        join.writeCompactNullableString(Some("uniform"))
        join.writeCompactNullableArray(Some(Vector.empty[Unit]))(_ => ())
        join.writeEmptyTaggedFields()
        request(output, input, join.result())

        val describe = requestHeader(ApiKey.ConsumerGroupDescribe, 1, 76, flexible = true)
        describe.writeCompactArray(Vector("described-group"))(describe.writeCompactString)
        describe.writeBoolean(true).writeEmptyTaggedFields()
        val response = request(output, input, describe.result())
        assertEquals(response.readInt(), 76)
        response.skipTaggedFields()
        assertEquals(response.readInt(), 0)
        assertEquals(response.readUnsignedVarInt(), 2)
        assertEquals(response.readShort(), Errors.None)
        assertEquals(response.readCompactNullableString(), None)
        assertEquals(response.readCompactString(), "described-group")
        assertEquals(response.readCompactString(), "Stable")
        assertEquals(response.readInt(), 1)
        assertEquals(response.readInt(), 1)
        assertEquals(response.readCompactString(), "uniform")
        assertEquals(response.readUnsignedVarInt(), 2)
        assertEquals(response.readCompactString(), "described-member")
        assertEquals(response.readCompactNullableString(), Some("instance-a"))
        assertEquals(response.readCompactNullableString(), Some("rack-a"))
        assertEquals(response.readInt(), 1)
        assertEquals(response.readCompactString(), "integration")
        assertEquals(response.readCompactString(), "127.0.0.1")
        assertEquals(response.readCompactArray(response.readCompactString()), Vector("described-events"))
        assertEquals(response.readCompactNullableString(), None)
        (0 until 2).foreach { _ =>
          assertEquals(response.readUnsignedVarInt(), 2)
          response.readUuid()
          assertEquals(response.readCompactString(), "described-events")
          assertEquals(response.readCompactArray(response.readInt()), Vector(0))
          response.skipTaggedFields()
          response.skipTaggedFields()
        }
        assertEquals(response.readByte(), 1.toByte)
        response.skipTaggedFields()
        assertNotEquals(response.readInt(), Int.MinValue)
        response.skipTaggedFields()
        response.skipTaggedFields()
        response.ensureFullyRead()
      finally socket.close()
    }
  }

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

  test("serves the OffsetFetch v4 response shape used by KafkaJS") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        val response = request(output, input, offsetFetchV4Request("node-readers", "node-events", correlationId = 17))
        assertEquals(response.readInt(), 17)
        assertEquals(response.readInt(), 0)
        assertEquals(response.readInt(), 1)
        assertEquals(response.readString(), "node-events")
        assertEquals(response.readInt(), 1)
        assertEquals(response.readInt(), 0)
        assertEquals(response.readLong(), -1L)
        assertEquals(response.readNullableString(), None)
        assertEquals(response.readShort(), Errors.None)
        assertEquals(response.readShort(), Errors.None)
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("serves flexible OffsetFetch v7 with stable reads") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("flex-offset-events", 81))
        request(output, input, offsetCommitV5Request("flex-offset-group", "flex-offset-events", 27L, 82))
        val fetch = requestHeader(ApiKey.OffsetFetch, 7, 83, flexible = true)
          .writeCompactString("flex-offset-group")
        fetch.writeCompactNullableArray(Some(Vector("flex-offset-events"))) { topic =>
          fetch.writeCompactString(topic).writeCompactArray(Vector(0))(fetch.writeInt).writeEmptyTaggedFields(): Unit
        }
        fetch.writeBoolean(true).writeEmptyTaggedFields()
        val response = request(output, input, fetch.result())
        assertEquals(response.readInt(), 83)
        response.skipTaggedFields()
        assertEquals(response.readInt(), 0)
        assertEquals(response.readUnsignedVarInt(), 2)
        assertEquals(response.readCompactString(), "flex-offset-events")
        assertEquals(response.readUnsignedVarInt(), 2)
        assertEquals(response.readInt(), 0)
        assertEquals(response.readLong(), 27L)
        assertEquals(response.readInt(), -1)
        assertEquals(response.readCompactNullableString(), None)
        assertEquals(response.readShort(), Errors.None)
        response.skipTaggedFields()
        response.skipTaggedFields()
        assertEquals(response.readShort(), Errors.None)
        response.skipTaggedFields()
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("accepts the OffsetCommit v5 request shape used by KafkaJS") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("node-events", correlationId = 16))
        val response = request(output, input, offsetCommitV5Request("node-readers", "node-events", 9L, correlationId = 18))
        assertEquals(response.readInt(), 18)
        assertEquals(response.readInt(), 0)
        assertEquals(response.readInt(), 1)
        assertEquals(response.readString(), "node-events")
        assertEquals(response.readInt(), 1)
        assertEquals(response.readInt(), 0)
        assertEquals(response.readShort(), Errors.None)
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("lists acknowledged offset groups with Kafka ListGroups v4 state filters") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("admin-events", correlationId = 60))
        request(output, input, offsetCommitV5Request("admin-readers", "admin-events", 9L, correlationId = 61))

        def listed(states: Vector[String], correlationId: Int): Vector[(String, String, String)] =
          val writer = requestHeader(ApiKey.ListGroups, 4, correlationId, flexible = true)
          writer.writeCompactArray(states)(writer.writeCompactString).writeEmptyTaggedFields()
          val response = request(output, input, writer.result())
          assertEquals(response.readInt(), correlationId)
          response.skipTaggedFields()
          assertEquals(response.readInt(), 0)
          assertEquals(response.readShort(), Errors.None)
          val groups = response.readCompactArray {
            val group = (response.readCompactString(), response.readCompactString(), response.readCompactString())
            response.skipTaggedFields()
            group
          }
          response.skipTaggedFields()
          response.ensureFullyRead()
          groups

        assertEquals(listed(Vector("Empty"), 62), Vector(("admin-readers", "", "Empty")))
        assertEquals(listed(Vector("Stable"), 63), Vector.empty)
      finally socket.close()
    }
  }

  test("commits offsets with flexible OffsetCommit v8") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("flex-commit-events", 88))
        val commit = requestHeader(ApiKey.OffsetCommit, 8, 89, flexible = true)
          .writeCompactString("flex-commit-group")
          .writeInt(-1)
          .writeCompactString("")
          .writeCompactNullableString(None)
        commit.writeCompactArray(Vector("flex-commit-events")) { topic =>
          commit.writeCompactString(topic)
          commit.writeCompactArray(Vector(0)) { partition =>
            commit.writeInt(partition).writeLong(41L).writeInt(3)
              .writeCompactNullableString(Some("checkpoint")).writeEmptyTaggedFields(): Unit
          }
          commit.writeEmptyTaggedFields(): Unit
        }
        commit.writeEmptyTaggedFields()
        val response = request(output, input, commit.result())
        assertEquals(response.readInt(), 89)
        response.skipTaggedFields()
        assertEquals(response.readInt(), 0)
        assertEquals(response.readUnsignedVarInt(), 2)
        assertEquals(response.readCompactString(), "flex-commit-events")
        assertEquals(response.readUnsignedVarInt(), 2)
        assertEquals(response.readInt(), 0)
        assertEquals(response.readShort(), Errors.None)
        response.skipTaggedFields()
        response.skipTaggedFields()
        response.skipTaggedFields()
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("fetches offsets for multiple groups with OffsetFetch v8") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("multi-offset-events", 84))
        request(output, input, offsetCommitV5Request("multi-a", "multi-offset-events", 31L, 85))
        request(output, input, offsetCommitV5Request("multi-b", "multi-offset-events", 32L, 86))
        val fetch = requestHeader(ApiKey.OffsetFetch, 8, 87, flexible = true)
        fetch.writeCompactArray(Vector("multi-a", "multi-b")) { groupId =>
          fetch.writeCompactString(groupId)
          fetch.writeCompactNullableArray(Some(Vector("multi-offset-events"))) { topic =>
            fetch.writeCompactString(topic).writeCompactArray(Vector(0))(fetch.writeInt).writeEmptyTaggedFields(): Unit
          }
          fetch.writeEmptyTaggedFields(): Unit
        }
        fetch.writeBoolean(false).writeEmptyTaggedFields()
        val response = request(output, input, fetch.result())
        assertEquals(response.readInt(), 87)
        response.skipTaggedFields()
        assertEquals(response.readInt(), 0)
        val groups = response.readCompactArray {
          val groupId = response.readCompactString()
          assertEquals(response.readUnsignedVarInt(), 2)
          assertEquals(response.readCompactString(), "multi-offset-events")
          assertEquals(response.readUnsignedVarInt(), 2)
          assertEquals(response.readInt(), 0)
          val offset = response.readLong()
          assertEquals(response.readInt(), -1)
          assertEquals(response.readCompactNullableString(), None)
          assertEquals(response.readShort(), Errors.None)
          response.skipTaggedFields()
          response.skipTaggedFields()
          assertEquals(response.readShort(), Errors.None)
          response.skipTaggedFields()
          groupId -> offset
        }
        assertEquals(groups, Vector("multi-a" -> 31L, "multi-b" -> 32L))
        response.skipTaggedFields()
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("describes acknowledged groups with Kafka DescribeGroups v4") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("describe-events", correlationId = 64))
        request(output, input, offsetCommitV5Request("describe-readers", "describe-events", 11L, correlationId = 65))

        val writer = requestHeader(ApiKey.DescribeGroups, 4, 66)
        writer.writeArray(Vector("describe-readers", "missing"))(writer.writeString).writeBoolean(true)
        val response = request(output, input, writer.result())
        assertEquals(response.readInt(), 66)
        assertEquals(response.readInt(), 0)
        val groups = response.readArray {
          val error = response.readShort()
          val groupId = response.readString()
          val state = response.readString()
          val protocolType = response.readString()
          response.readString()
          val members = response.readArray {
            response.readString()
            response.readNullableString()
            response.readString()
            response.readString()
            response.readByteArray()
            response.readByteArray()
          }
          val operations = response.readInt()
          (groupId, error, state, protocolType, members.size, operations)
        }
        assertEquals(groups.head, ("describe-readers", Errors.None, "Empty", "", 0, 328))
        assertEquals(groups(1), ("missing", Errors.GroupIdNotFound, "", "", 0, Int.MinValue))
        response.ensureFullyRead()
      finally socket.close()
    }
  }

  test("deletes empty groups and their offsets with Kafka DeleteGroups v1 and v2") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("delete-events", correlationId = 67))
        request(output, input, offsetCommitV5Request("delete-readers", "delete-events", 13L, correlationId = 68))

        val delete = requestHeader(ApiKey.DeleteGroups, 1, 69)
        delete.writeArray(Vector("delete-readers", "missing"))(delete.writeString)
        val deleted = request(output, input, delete.result())
        assertEquals(deleted.readInt(), 69)
        assertEquals(deleted.readInt(), 0)
        assertEquals(
          deleted.readArray((deleted.readString(), deleted.readShort())),
          Vector("delete-readers" -> Errors.None, "missing" -> Errors.GroupIdNotFound)
        )
        deleted.ensureFullyRead()

        request(output, input, offsetCommitV5Request("delete-readers-v2", "delete-events", 14L, correlationId = 71))
        val flexibleDelete = requestHeader(ApiKey.DeleteGroups, 2, 72, flexible = true)
        flexibleDelete.writeCompactArray(Vector("delete-readers-v2"))(flexibleDelete.writeCompactString)
        flexibleDelete.writeEmptyTaggedFields()
        val flexiblyDeleted = request(output, input, flexibleDelete.result())
        assertEquals(flexiblyDeleted.readInt(), 72)
        flexiblyDeleted.skipTaggedFields()
        assertEquals(flexiblyDeleted.readInt(), 0)
        assertEquals(flexiblyDeleted.readUnsignedVarInt(), 2)
        assertEquals(flexiblyDeleted.readCompactString(), "delete-readers-v2")
        assertEquals(flexiblyDeleted.readShort(), Errors.None)
        flexiblyDeleted.skipTaggedFields()
        flexiblyDeleted.skipTaggedFields()
        flexiblyDeleted.ensureFullyRead()

        val list = requestHeader(ApiKey.ListGroups, 4, 70, flexible = true)
        list.writeCompactArray(Vector("Empty"))(list.writeCompactString).writeEmptyTaggedFields()
        val listed = request(output, input, list.result())
        assertEquals(listed.readInt(), 70)
        listed.skipTaggedFields()
        assertEquals(listed.readInt(), 0)
        assertEquals(listed.readShort(), Errors.None)
        assertEquals(listed.readCompactArray {
          val groupId = listed.readCompactString()
          listed.readCompactString()
          listed.readCompactString()
          listed.skipTaggedFields()
          groupId
        }, Vector.empty)
        listed.skipTaggedFields()
        listed.ensureFullyRead()
      finally socket.close()
    }
  }

  test("deletes selected committed offsets with OffsetDelete v0") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("offset-delete-events", correlationId = 77))
        request(output, input, offsetCommitV5Request("offset-delete-group", "offset-delete-events", 19L, correlationId = 78))

        val delete = requestHeader(ApiKey.OffsetDelete, 0, 79)
          .writeString("offset-delete-group")
        delete.writeArray(Vector("offset-delete-events")) { topic =>
          delete.writeString(topic).writeArray(Vector(0))(delete.writeInt): Unit
        }
        val response = request(output, input, delete.result())
        assertEquals(response.readInt(), 79)
        assertEquals(response.readShort(), Errors.None)
        assertEquals(response.readInt(), 0)
        assertEquals(response.readArray {
          val topic = response.readString()
          val partitions = response.readArray((response.readInt(), response.readShort()))
          topic -> partitions
        }, Vector("offset-delete-events" -> Vector(0 -> Errors.None)))
        response.ensureFullyRead()

        val fetched = request(output, input, offsetFetchV4Request("offset-delete-group", "offset-delete-events", correlationId = 80))
        assertEquals(fetched.readInt(), 80)
        assertEquals(fetched.readInt(), 0)
        assertEquals(fetched.readInt(), 1)
        assertEquals(fetched.readString(), "offset-delete-events")
        assertEquals(fetched.readInt(), 1)
        assertEquals(fetched.readInt(), 0)
        assertEquals(fetched.readLong(), -1L)
        assertEquals(fetched.readNullableString(), None)
        assertEquals(fetched.readShort(), Errors.None)
        assertEquals(fetched.readShort(), Errors.None)
        fetched.ensureFullyRead()
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

  test("applies response, Produce, and Fetch quotas and reports Kafka throttle fields") {
    val directory = Files.createTempDirectory("cascade-traffic-quotas")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory,
        security = cascade.security.BrokerSecurityConfig(
          resources = ResourceLimits(
            responseBytesPerSecond = 1_000_000L,
            responseBurstBytes = 1L,
            produceBytesPerSecond = 1_000_000L,
            produceBurstBytes = 1L,
            fetchBytesPerSecond = 1_000_000L,
            fetchBurstBytes = 1L,
            maxThrottleMillis = 100L
          )
        )
      )
    )
    try
      broker.start()
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("quota-events", 1))

        val produced = request(output, input, produceRequest("quota-events", TestRecordBatch.single(), 2))
        produced.readInt()
        produced.readArray {
          produced.readString()
          produced.readArray {
            produced.readInt()
            produced.readShort()
            produced.readLong()
            produced.readLong()
          }
        }
        assert(produced.readInt() > 0)
        produced.ensureFullyRead()

        val fetched = request(output, input, fetchRequest("quota-events", 3))
        fetched.readInt()
        assert(fetched.readInt() > 0)
        assert(broker.responseQuotaSnapshot.throttled >= 3L)
        assertEquals(broker.responseQuotaSnapshot.rejected, 0L)
        assertEquals(broker.produceQuotaSnapshot.throttled, 1L)
        assertEquals(broker.fetchQuotaSnapshot.throttled, 1L)
      finally socket.close()
    finally
      broker.close()
      deleteTree(directory)
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
        val batching = resources.head._4.filter(_._1.startsWith("cascade.offset.batch."))
          .map(value => value._1 -> value._2.getOrElse("")).toMap
        assertEquals(batching, Map(
          "cascade.offset.batch.max.requests" -> "64", "cascade.offset.batch.max.bytes" -> "1048576",
          "cascade.offset.batch.pending.requests" -> "1024", "cascade.offset.batch.pending.bytes" -> "16777216",
          "cascade.offset.batch.linger.ms" -> "2", "cascade.offset.batch.queue.timeout.ms" -> "5000"))
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

  test("serves flexible transaction and producer administration from live delivery state") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("delivery-admin", correlationId = 90))

        val initialized = request(
          output,
          input,
          requestHeader(ApiKey.InitProducerId, 1, 91)
            .writeNullableString(Some("admin-transaction"))
            .writeInt(30_000)
            .result()
        )
        assertEquals(initialized.readInt(), 91)
        initialized.readInt()
        assertEquals(initialized.readShort(), Errors.None)
        val producerId = initialized.readLong()
        val producerEpoch = initialized.readShort()
        initialized.ensureFullyRead()

        val add = requestHeader(ApiKey.AddPartitionsToTxn, 1, 92)
          .writeString("admin-transaction")
          .writeLong(producerId)
          .writeShort(producerEpoch)
        add.writeArray(Vector("delivery-admin")) { topic =>
          add.writeString(topic).writeArray(Vector(0))(add.writeInt): Unit
        }
        val added = request(output, input, add.result())
        assertEquals(added.readInt(), 92)
        added.readInt()
        assertEquals(added.readInt(), 1)
        assertEquals(added.readString(), "delivery-admin")
        assertEquals(added.readInt(), 1)
        assertEquals(added.readInt(), 0)
        assertEquals(added.readShort(), Errors.None)
        added.ensureFullyRead()

        val records = TestRecordBatch.producer(producerId, producerEpoch, 0, transactional = true)
        val produce = requestHeader(ApiKey.Produce, 3, 93)
          .writeNullableString(Some("admin-transaction"))
          .writeShort(1)
          .writeInt(30_000)
        produce.writeArray(Vector("delivery-admin")) { topic =>
          produce.writeString(topic)
          produce.writeArray(Vector(0)) { partition =>
            produce.writeInt(partition).writeNullableBytes(Some(records)): Unit
          }: Unit
        }
        assertProduceResult(request(output, input, produce.result()), 93, Errors.None, 0L, "delivery-admin")

        val describe = requestHeader(ApiKey.DescribeTransactions, 0, 94, flexible = true)
        describe.writeCompactArray(Vector("admin-transaction"))(describe.writeCompactString).writeEmptyTaggedFields()
        val described = request(output, input, describe.result())
        assertEquals(described.readInt(), 94)
        described.skipTaggedFields()
        assertEquals(described.readInt(), 0)
        assertEquals(described.readUnsignedVarInt(), 2)
        assertEquals(described.readShort(), Errors.None)
        assertEquals(described.readCompactString(), "admin-transaction")
        assertEquals(described.readCompactString(), "Ongoing")
        assertEquals(described.readInt(), 30_000)
        assert(described.readLong() > 0L)
        assertEquals(described.readLong(), producerId)
        assertEquals(described.readShort(), producerEpoch)
        assertEquals(described.readUnsignedVarInt(), 2)
        assertEquals(described.readCompactString(), "delivery-admin")
        assertEquals(described.readUnsignedVarInt(), 2)
        assertEquals(described.readInt(), 0)
        described.skipTaggedFields()
        described.skipTaggedFields()
        described.skipTaggedFields()
        described.ensureFullyRead()

        val list = requestHeader(ApiKey.ListTransactions, 2, 95, flexible = true)
        list.writeCompactArray(Vector("Ongoing"))(list.writeCompactString)
        list.writeCompactArray(Vector(producerId))(list.writeLong)
        list.writeLong(-1L).writeCompactNullableString(Some("admin-.*")).writeEmptyTaggedFields()
        val listed = request(output, input, list.result())
        assertEquals(listed.readInt(), 95)
        listed.skipTaggedFields()
        listed.readInt()
        assertEquals(listed.readShort(), Errors.None)
        assertEquals(listed.readUnsignedVarInt(), 1)
        assertEquals(listed.readUnsignedVarInt(), 2)
        assertEquals(listed.readCompactString(), "admin-transaction")
        assertEquals(listed.readLong(), producerId)
        assertEquals(listed.readCompactString(), "Ongoing")
        listed.skipTaggedFields()
        listed.skipTaggedFields()
        listed.ensureFullyRead()

        val describeProducers = requestHeader(ApiKey.DescribeProducers, 0, 96, flexible = true)
        describeProducers.writeCompactArray(Vector("delivery-admin")) { topic =>
          describeProducers.writeCompactString(topic)
          describeProducers.writeCompactArray(Vector(0))(describeProducers.writeInt)
          describeProducers.writeEmptyTaggedFields(): Unit
        }
        describeProducers.writeEmptyTaggedFields()
        val producerState = request(output, input, describeProducers.result())
        assertEquals(producerState.readInt(), 96)
        producerState.skipTaggedFields()
        producerState.readInt()
        assertEquals(producerState.readUnsignedVarInt(), 2)
        assertEquals(producerState.readCompactString(), "delivery-admin")
        assertEquals(producerState.readUnsignedVarInt(), 2)
        assertEquals(producerState.readInt(), 0)
        assertEquals(producerState.readShort(), Errors.None)
        assertEquals(producerState.readCompactNullableString(), None)
        assertEquals(producerState.readUnsignedVarInt(), 2)
        assertEquals(producerState.readLong(), producerId)
        assertEquals(producerState.readInt(), producerEpoch.toInt)
        assertEquals(producerState.readInt(), 0)
        assertEquals(producerState.readLong(), 0L)
        assertEquals(producerState.readInt(), -1)
        assertEquals(producerState.readLong(), 0L)
        producerState.skipTaggedFields()
        producerState.skipTaggedFields()
        producerState.skipTaggedFields()
        producerState.skipTaggedFields()
        producerState.ensureFullyRead()
      finally socket.close()
    }
  }

  test("serves batched verify-only transaction partition admission") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        request(output, input, metadataRequest("delivery-verify", correlationId = 97))
        request(output, input, metadataRequest("delivery-verify-other", correlationId = 98))

        val init = requestHeader(ApiKey.InitProducerId, 4, 99, flexible = true)
          .writeCompactNullableString(Some("delivery-verify-txn"))
          .writeInt(30_000)
          .writeLong(-1L)
          .writeShort((-1).toShort)
          .writeEmptyTaggedFields()
        val initialized = request(output, input, init.result())
        assertEquals(initialized.readInt(), 99)
        initialized.skipTaggedFields()
        initialized.readInt()
        assertEquals(initialized.readShort(), Errors.None)
        val producerId = initialized.readLong()
        val producerEpoch = initialized.readShort()
        initialized.skipTaggedFields()

        def partitionsRequest(correlationId: Int, verifyOnly: Boolean, topicName: String = "delivery-verify"): Array[Byte] =
          val writer = requestHeader(ApiKey.AddPartitionsToTxn, 5, correlationId, flexible = true)
          writer.writeCompactArray(Vector("delivery-verify-txn")) { transactionalId =>
            writer.writeCompactString(transactionalId).writeLong(producerId).writeShort(producerEpoch).writeBoolean(verifyOnly)
            writer.writeCompactArray(Vector(topicName)) { topic =>
              writer.writeCompactString(topic)
              writer.writeCompactArray(Vector(0))(writer.writeInt)
              writer.writeEmptyTaggedFields(): Unit
            }
            writer.writeEmptyTaggedFields(): Unit
          }
          writer.writeEmptyTaggedFields().result()

        def readPartitionError(response: ByteCursor, correlationId: Int, topicName: String = "delivery-verify"): Short =
          assertEquals(response.readInt(), correlationId)
          response.skipTaggedFields()
          response.readInt()
          assertEquals(response.readShort(), Errors.None)
          assertEquals(response.readUnsignedVarInt(), 2)
          assertEquals(response.readCompactString(), "delivery-verify-txn")
          assertEquals(response.readUnsignedVarInt(), 2)
          assertEquals(response.readCompactString(), topicName)
          assertEquals(response.readUnsignedVarInt(), 2)
          assertEquals(response.readInt(), 0)
          val error = response.readShort()
          response.skipTaggedFields()
          response.skipTaggedFields()
          response.skipTaggedFields()
          response.skipTaggedFields()
          response.ensureFullyRead()
          error

        assertEquals(readPartitionError(request(output, input, partitionsRequest(100, verifyOnly = true)), 100), Errors.InvalidTxnState)
        assertEquals(readPartitionError(request(output, input, partitionsRequest(101, verifyOnly = false)), 101), Errors.None)
        assertEquals(readPartitionError(request(output, input, partitionsRequest(102, verifyOnly = true)), 102), Errors.None)
        assertEquals(
          readPartitionError(
            request(output, input, partitionsRequest(103, verifyOnly = true, topicName = "delivery-verify-other")),
            103,
            "delivery-verify-other"
          ),
          Errors.TransactionAbortable
        )
      finally socket.close()
    }
  }

  test("serves resumable flexible producer initialization without double epoch bumps") {
    withBroker { broker =>
      val socket = Socket("127.0.0.1", broker.boundPort)
      try
        val input = DataInputStream(BufferedInputStream(socket.getInputStream))
        val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
        def initialize(correlationId: Int, producerId: Long, producerEpoch: Short): (Short, Long, Short) =
          val writer = requestHeader(ApiKey.InitProducerId, 4, correlationId, flexible = true)
            .writeCompactNullableString(Some("resumable-producer"))
            .writeInt(30_000)
            .writeLong(producerId)
            .writeShort(producerEpoch)
            .writeEmptyTaggedFields()
          val response = request(output, input, writer.result())
          assertEquals(response.readInt(), correlationId)
          response.skipTaggedFields()
          response.readInt()
          val result = (response.readShort(), response.readLong(), response.readShort())
          response.skipTaggedFields()
          response.ensureFullyRead()
          result

        val first = initialize(97, -1L, -1)
        assertEquals(first._1, Errors.None)
        val bumped = initialize(98, first._2, first._3)
        assertEquals(bumped, (Errors.None, first._2, 1.toShort))
        assertEquals(initialize(99, first._2, first._3), bumped)
        assertEquals(initialize(100, first._2, 7.toShort)._1, Errors.ProducerFenced)
      finally socket.close()
    }
  }

  private def apiVersionsRequest(correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.ApiVersions, 4, correlationId, flexible = true)
    writer.writeCompactString("cascade-test").writeCompactString("1.0").writeEmptyTaggedFields().result()

  private def metadataRequest(topic: String, correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.Metadata, 4, correlationId)
    writer.writeArray(Vector(topic))(writer.writeString).writeBoolean(true).result()

  private def offsetFetchV4Request(groupId: String, topic: String, correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.OffsetFetch, 4, correlationId).writeString(groupId)
    writer.writeNullableArray(Some(Vector(topic))) { name =>
      writer.writeString(name)
      writer.writeArray(Vector(0))(writer.writeInt): Unit
    }
    writer.result()

  private def offsetCommitV5Request(groupId: String, topic: String, offset: Long, correlationId: Int): Array[Byte] =
    val writer = requestHeader(ApiKey.OffsetCommit, 5, correlationId)
      .writeString(groupId)
      .writeInt(-1)
      .writeString("")
    writer.writeArray(Vector(topic)) { name =>
      writer.writeString(name)
      writer.writeArray(Vector(0)) { partition =>
        writer.writeInt(partition).writeLong(offset).writeNullableString(None): Unit
      }: Unit
    }
    writer.result()

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

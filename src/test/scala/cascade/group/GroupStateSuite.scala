package cascade.group

import munit.FunSuite

final class GroupStateSuite extends FunSuite:
  test("classic-only group images keep the rolling-compatible format") {
    val encoded = GroupCodec.encode(GroupImage.Empty)
    assertEquals(cascade.protocol.ByteCursor(encoded).readShort(), 1.toShort)
    assertEquals(GroupCodec.decode(encoded), GroupImage.Empty)
  }

  test("group images preserve membership, assignments, pending identities, and offsets") {
    val image = GroupImage(
      version = 12L,
      groups = Vector(
        StoredGroup(
          groupId = "analytics",
          status = GroupStatus.Stable,
          generationId = 4,
          leaderId = "consumer-a",
          protocolType = "consumer",
          protocolName = "range",
          rebalanceDeadlineMillis = 9000L,
          members = Vector(
            StoredMember(
              memberId = "consumer-a",
              groupInstanceId = Some("instance-a"),
              sessionTimeoutMillis = 10000,
              rebalanceTimeoutMillis = 30000,
              protocols = Vector(StoredProtocol("range", Vector[Byte](1, 2, 3))),
              clientId = "load-reader",
              lastHeartbeatMillis = 8000L,
              assignment = Vector[Byte](4, 5, 6)
            )
          ),
          joined = Vector("consumer-a"),
          pendingMemberIds = Vector("consumer-b" -> 10000L)
        )
      ),
      offsets = Vector(
        OffsetCommitValue(
          GroupOffsetKey("analytics", "events", 2),
          CommittedOffset(41L, 3, Some("checkpoint"), 7000L)
        )
      ),
      consumerGroups = Vector(
        StoredConsumerGroup(
          "modern",
          7,
          Vector(
            StoredConsumerMember(
              "member-a",
              Some("instance-a"),
              Some("rack-a"),
              30_000,
              Vector("events"),
              "uniform",
              7,
              8_000L,
              Vector(ConsumerTopicPartitions(ConsumerTopicId(10L, 20L), Vector(0))),
              Some("events-.*"),
              "consumer-client",
              "10.0.0.8",
              Vector(ConsumerTopicPartitions(ConsumerTopicId(10L, 20L), Vector(0, 2)))
            )
          ),
          7
        )
      )
    )

    val encoded = GroupCodec.encode(image)
    assertEquals(cascade.protocol.ByteCursor(encoded).readShort(), 3.toShort)
    assertEquals(GroupCodec.decode(encoded), image)
  }

  test("group images reject unknown formats") {
    val bytes = GroupCodec.encode(GroupImage.Empty)
    bytes(1) = 4
    interceptMessage[cascade.protocol.ProtocolException]("unsupported group-state format: 4") {
      GroupCodec.decode(bytes)
    }
  }

  test("format two consumer groups upgrade with safe assignment defaults") {
    val writer = cascade.protocol.ByteWriter().writeShort(2).writeLong(9L)
    writer.writeArray(Vector.empty[Unit])(_ => ())
    writer.writeArray(Vector.empty[Unit])(_ => ())
    writer.writeArray(Vector("legacy-modern")) { groupId =>
      writer.writeString(groupId).writeInt(4)
      writer.writeArray(Vector("member-a")) { memberId =>
        writer.writeString(memberId)
        writer.writeNullableString(None).writeNullableString(None)
        writer.writeInt(30_000)
        writer.writeArray(Vector("events"))(writer.writeString)
        writer.writeString("uniform").writeInt(4).writeLong(1000L)
        writer.writeArray(Vector(ConsumerTopicPartitions(ConsumerTopicId(1L, 2L), Vector(0)))) { topic =>
          writer.writeUuid(topic.topicId.mostSignificantBits, topic.topicId.leastSignificantBits)
          writer.writeArray(topic.partitions)(writer.writeInt): Unit
        }: Unit
      }: Unit
    }

    val decoded = GroupCodec.decode(writer.result())
    val group = decoded.consumerGroups.head
    assertEquals(group.assignmentEpoch, group.groupEpoch)
    assertEquals(group.members.head.targetAssignment, group.members.head.assignment)
    assertEquals(group.members.head.subscribedTopicRegex, None)
  }

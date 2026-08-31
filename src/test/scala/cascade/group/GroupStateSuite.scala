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
              Vector(ConsumerTopicPartitions(ConsumerTopicId(10L, 20L), Vector(0, 2)))
            )
          )
        )
      )
    )

    val encoded = GroupCodec.encode(image)
    assertEquals(cascade.protocol.ByteCursor(encoded).readShort(), 2.toShort)
    assertEquals(GroupCodec.decode(encoded), image)
  }

  test("group images reject unknown formats") {
    val bytes = GroupCodec.encode(GroupImage.Empty)
    bytes(1) = 3
    interceptMessage[cascade.protocol.ProtocolException]("unsupported group-state format: 3") {
      GroupCodec.decode(bytes)
    }
  }

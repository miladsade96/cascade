package cascade.group

import munit.FunSuite

final class GroupStateSuite extends FunSuite:
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
      )
    )

    assertEquals(GroupCodec.decode(GroupCodec.encode(image)), image)
  }

  test("group images reject unknown formats") {
    val bytes = GroupCodec.encode(GroupImage.Empty)
    bytes(1) = 2
    interceptMessage[cascade.protocol.ProtocolException]("unsupported group-state format: 2") {
      GroupCodec.decode(bytes)
    }
  }

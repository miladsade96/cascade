package cascade.group

import munit.FunSuite

final class GroupAdminReadViewSuite extends FunSuite:
  test("indexes a classic group with its selected metadata and assignment") {
    val member = StoredMember(
      "member-1",
      Some("instance-1"),
      10000,
      30000,
      Vector(StoredProtocol("range", Vector[Byte](1, 2)), StoredProtocol("other", Vector[Byte](9))),
      "client-1",
      1000L,
      Vector[Byte](3, 4)
    )
    val group = StoredGroup(
      "workers",
      GroupStatus.Stable,
      4,
      member.memberId,
      "consumer",
      "range",
      2000L,
      Vector(member),
      Vector(member.memberId),
      Vector.empty
    )

    val view = GroupAdminReadView.from(GroupImage(7L, Vector(group), Vector.empty))
    val description = view.get("workers").getOrElse(fail("missing group"))

    assertEquals(view.imageVersion, 7L)
    assertEquals(description.state, "Stable")
    assertEquals(description.protocolType, "consumer")
    assertEquals(description.protocolData, "range")
    assertEquals(description.members.map(_.metadata), Vector(Vector[Byte](1, 2)))
    assertEquals(description.members.map(_.assignment), Vector(Vector[Byte](3, 4)))
    assertEquals(description.members.map(_.groupInstanceId), Vector(Some("instance-1")))
  }

  test("lists consumer-protocol and offset-only groups deterministically") {
    val consumer = StoredConsumerGroup(
      "z-consumer",
      3,
      Vector(StoredConsumerMember(
        "member-2",
        None,
        Some("rack-a"),
        30000,
        Vector("events"),
        "uniform",
        2,
        1000L,
        Vector.empty
      ))
    )
    val offset = OffsetCommitValue(
      GroupOffsetKey("a-offsets", "events", 0),
      CommittedOffset(10L, -1, None, 1000L)
    )

    val view = GroupAdminReadView.from(GroupImage(8L, Vector.empty, Vector(offset), Vector(consumer)))

    assertEquals(view.groups.map(_.groupId), Vector("a-offsets", "z-consumer"))
    assertEquals(view.get("a-offsets").map(_.state), Some("Empty"))
    assertEquals(view.get("a-offsets").map(_.protocolType), Some(""))
    assertEquals(view.get("z-consumer").map(_.state), Some("Stable"))
    assertEquals(view.get("z-consumer").map(_.protocolType), Some("consumer"))
    assertEquals(view.get("z-consumer").map(_.protocolData), Some("uniform"))
  }

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

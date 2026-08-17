package cascade.cluster

class QuorumMembershipSuite extends munit.FunSuite:
  private val nodes = Vector(
    ClusterNode(1, "node-1", 9092),
    ClusterNode(2, "node-2", 9093),
    ClusterNode(3, "node-3", 9094),
    ClusterNode(4, "node-4", 9095)
  )

  test("stable membership requires a simple majority") {
    val membership = QuorumMembership.bootstrap(nodes.take(3))

    assert(!membership.hasQuorum(Set(1)))
    assert(membership.hasQuorum(Set(1, 2)))
    assert(membership.hasQuorum(Set(2, 3)))
  }

  test("joint membership requires majorities of both voter sets") {
    val stable = QuorumMembership.bootstrap(nodes.take(3))
    val joint = stable.beginTransition(QuorumMembership.bootstrap(nodes).currentVoters)

    assert(!joint.hasQuorum(Set(1, 2)))
    assert(joint.hasQuorum(Set(1, 3, 4)))
    assert(joint.hasQuorum(Set(1, 2, 4)))
  }

  test("removal cannot be committed by only the old or new majority") {
    val stable = QuorumMembership.bootstrap(nodes.take(3))
    val joint = stable.beginTransition(stable.currentVoters.filterNot(_.id == 3))

    assert(!joint.hasQuorum(Set(1)))
    assert(!joint.hasQuorum(Set(2, 3)))
    assert(joint.hasQuorum(Set(1, 2)))
    assertEquals(joint.stabilize.currentVoters.map(_.id), Vector(1, 2))
  }

  test("bootstrap directory IDs are stable and node-specific") {
    val first = VoterDirectoryId.bootstrap(1)

    assertEquals(first, VoterDirectoryId.bootstrap(1))
    assertNotEquals(first, VoterDirectoryId.bootstrap(2))
    assert(!first.isZero)
  }

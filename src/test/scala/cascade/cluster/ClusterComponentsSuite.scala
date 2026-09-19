package cascade.cluster

import cascade.coordinator.CoordinatorKey
import munit.FunSuite

final class ClusterComponentsSuite extends FunSuite:
  private val nodes = Vector(
    ClusterNode(1, "broker-1", 9092),
    ClusterNode(2, "broker-2", 9093),
    ClusterNode(3, "broker-3", 9094)
  )
  private val membership = QuorumMembership.bootstrap(nodes)

  test("broker membership prefers the committed image and retains bootstrap discovery") {
    val view = BrokerMembership(nodes, nodes.head)
    val transitioned = QuorumMembership(Vector(QuorumVoter.bootstrap(nodes(1)), QuorumVoter.bootstrap(nodes(2))))
    val metadata = ClusterMetadata.Empty.copy(membership = Some(transitioned))

    assertEquals(view.nodes(metadata).map(_.id), Vector(2, 3))
    assertEquals(view.knownNode(metadata, 1), Some(nodes.head))
    assertEquals(view.activeNodeIds(metadata), Set(2, 3))
  }

  test("controller election gives the preferred initial broker the earliest deadline") {
    val preferred = ControllerElection(1, 1, heartbeatMillis = 100, electionTimeoutMillis = 600)
    val follower = ControllerElection(2, 1, heartbeatMillis = 100, electionTimeoutMillis = 600)
    val now = 10_000L
    assert(preferred.deadlineNanos(now, initial = true, 0L, membership) <
      follower.deadlineNanos(now, initial = true, 0L, membership))
  }

  test("controller lease expires exactly at the election timeout") {
    val election = ControllerElection(1, 1, heartbeatMillis = 100, electionTimeoutMillis = 600)
    val contact = 1_000_000L
    assert(election.hasLease(contact, contact + 599_999_999L))
    assert(!election.hasLease(contact, contact + 600_000_000L))
    assert(!election.hasLease(0L, contact))
  }

  test("coordinator router excludes unavailable voters") {
    val key = CoordinatorKey.group("orders")
    val selected = CoordinatorRouter.owner(
      key,
      clusterEnabled = true,
      nodes.head,
      Some(nodes.head),
      membership,
      unavailableBrokerIds = Set(1, 2),
      shardingEnabled = true,
      failoverEnabled = true
    )
    assertEquals(selected, Some(nodes(2)))
  }

  test("partition leadership promotes a surviving ISR member") {
    val partition = PartitionMetadata(0, 1, 7, Vector(1, 2, 3), Vector(1, 2, 3))
    val promoted = PartitionLeadership.removeFailedReplica(partition, 1)
    assertEquals(promoted.leaderId, 2)
    assertEquals(promoted.leaderEpoch, 8)
    assertEquals(promoted.inSyncReplicas, Vector(2, 3))
  }

  test("partition leadership finalizes a caught-up reassignment") {
    val partition = PartitionMetadata(
      0,
      1,
      4,
      Vector(1, 2, 3),
      Vector(1, 2, 3),
      addingReplicas = Vector(3),
      removingReplicas = Vector(1)
    )
    val finalized = PartitionLeadership.finalizeReassignment(partition)
    assertEquals(finalized.replicas, Vector(2, 3))
    assertEquals(finalized.leaderId, 2)
    assertEquals(finalized.leaderEpoch, 5)
    assert(!finalized.isReassigning)
  }

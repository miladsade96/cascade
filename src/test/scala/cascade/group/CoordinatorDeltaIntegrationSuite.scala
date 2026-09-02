package cascade.group

import cascade.coordinator.*
import cascade.delivery.*
import cascade.fault.FaultCluster
import cascade.protocol.Errors
import munit.FunSuite

final class CoordinatorDeltaIntegrationSuite extends FunSuite:
  private def payload(group: String, offset: Long): Vector[Byte] =
    val value = OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, System.currentTimeMillis()))
    GroupShardCodec.split(GroupCodec.encode(GroupImage(0L, Vector.empty, Vector(value))).toVector)(CoordinatorShard.group(group))

  test("a live quorum merges independent stale-global-version proposals and recovers them after controller loss") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val (term, _, metadata) = CoordinatorProbe.snapshot(controller)
      val a = "group-a"
      val b = Iterator.from(0).map(i => s"group-b-$i").find(id => CoordinatorShard.group(id) != CoordinatorShard.group(a)).get
      def delta(group: String, offset: Long) = CoordinatorDelta(term, Vector(CoordinatorShardUpdate(
        CoordinatorShard.group(group), metadata.coordinator.shardVersion(CoordinatorShard.group(group)), payload(group, offset)
      )))
      assertEquals(CoordinatorProbe.commit(controller, delta(a, 10L)), Errors.None)
      assertEquals(CoordinatorProbe.commit(controller, delta(b, 20L)), Errors.None)
      assertEquals(CoordinatorProbe.commit(controller, delta(a, 99L)), Errors.CoordinatorLoadInProgress)
      val state = CoordinatorProbe.snapshot(controller)._3.coordinator
      val offsets = GroupCodec.decode(state.groupState.toArray).offsets.map(v => v.key.groupId -> v.value.offset).toMap
      assertEquals(offsets, Map(a -> 10L, b -> 20L))
      cluster.stop(controller.id)
      val successor = CoordinatorProbe.controller(cluster.nodes, Set(controller.id))
      val recovered = CoordinatorProbe.snapshot(successor)._3.coordinator
      assertEquals(recovered.groupState, state.groupState)
      assertEquals(recovered.shardVersions, state.shardVersions)
      val staleTerm = delta(b, 30L).copy(updates = Vector(CoordinatorShardUpdate(
        CoordinatorShard.group(b), recovered.shardVersion(CoordinatorShard.group(b)), payload(b, 30L)
      )))
      assertEquals(CoordinatorProbe.commit(successor, staleTerm), Errors.CoordinatorLoadInProgress)
    finally cluster.close()
  }

  test("transaction outcome and consumer offsets either both commit or neither commits") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val (term, _, initial) = CoordinatorProbe.snapshot(controller)
      val group = "transaction-workers"
      val registration = ProducerRegistration(1L, 0, Some("orders"), 10000)
      val pending = PendingOffset(group, "events", 0, 20L, -1, None)
      val active = ActiveTransaction("orders", 1L, 0, 10000, System.currentTimeMillis(), Vector.empty, Vector.empty, Vector(group), Vector(pending))
      val delivery = DeliveryImage(1L, 2L, Vector(registration), Vector(active), Vector.empty)
      val before = CoordinatorShardState.payloads(initial.coordinator.groupState, initial.coordinator.deliveryState)
      val start = CoordinatorShardState.payloads(Vector.empty, DeliveryCodec.encode(delivery).toVector)
      assertEquals(CoordinatorProbe.commit(controller, CoordinatorShardState.changes(initial.coordinator, before, start, term).get), Errors.None)
      val base = CoordinatorProbe.snapshot(controller)._3.coordinator
      val groupShard = CoordinatorShard.group(group)
      val conflicting = CoordinatorDelta(term, Vector(CoordinatorShardUpdate(groupShard, base.shardVersion(groupShard), payload(group, 15L))))
      assertEquals(CoordinatorProbe.commit(controller, conflicting), Errors.None)
      val completed = CompletedTransaction("orders", 1L, 0, committed = true, offsetsApplied = true, Vector.empty, Vector(pending))
      val ended = delivery.copy(activeTransactions = Vector.empty, completedTransactions = Vector(completed))
      val desiredGroups = GroupShardCodec.split(Vector.empty).updated(groupShard, payload(group, 20L))
      val desired = desiredGroups ++ DeliveryShardCodec.split(DeliveryCodec.encode(ended).toVector)
      val stale = CoordinatorShardState.changes(base, CoordinatorShardState.payloads(base.groupState, base.deliveryState), desired, term).get
      assertEquals(CoordinatorProbe.commit(controller, stale), Errors.CoordinatorLoadInProgress)
      val rejected = CoordinatorProbe.snapshot(controller)._3.coordinator
      assertEquals(GroupCodec.decode(rejected.groupState.toArray).offsets.head.value.offset, 15L)
      assertEquals(DeliveryCodec.decode(rejected.deliveryState.toArray).activeTransactions, Vector(active))
      assertEquals(DeliveryCodec.decode(rejected.deliveryState.toArray).completedTransactions, Vector.empty)
      val rebased = CoordinatorShardState.changes(rejected, CoordinatorShardState.payloads(rejected.groupState, rejected.deliveryState), desired, term).get
      assertEquals(CoordinatorProbe.commit(controller, rebased), Errors.None)
      val accepted = CoordinatorProbe.snapshot(controller)._3.coordinator
      assertEquals(GroupCodec.decode(accepted.groupState.toArray).offsets.head.value.offset, 20L)
      assertEquals(DeliveryCodec.decode(accepted.deliveryState.toArray).activeTransactions, Vector.empty)
      assertEquals(DeliveryCodec.decode(accepted.deliveryState.toArray).completedTransactions, Vector(completed))
    finally cluster.close()
  }

package cascade.group

import cascade.coordinator.*
import cascade.fault.FaultCluster
import cascade.protocol.Errors
import munit.FunSuite

final class CoordinatorDeltaIntegrationSuite extends FunSuite:
  private def payload(group: String, offset: Long): Vector[Byte] =
    val value = OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, 1L))
    GroupShardCodec.split(GroupCodec.encode(GroupImage(0L, Vector.empty, Vector(value))).toVector)(CoordinatorShard.group(group))

  test("a live quorum merges independent stale-global-version proposals and recovers them after controller loss") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
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

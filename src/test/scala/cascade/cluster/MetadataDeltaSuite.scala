package cascade.cluster

import cascade.coordinator.*
import cascade.group.*
import munit.FunSuite

object MetadataDeltaFixture:
  val base: ClusterMetadata = ClusterMetadata.Empty.copy(version = 10L, controllerTerm = 4L,
    featureLevels = Map(ClusterFeature.CoordinatorDeltas -> 1.toShort, ClusterFeature.IncrementalCoordinator -> 1.toShort))

  def update(base: ClusterMetadata, group: String = "workers", offset: Long = 1L): (MetadataDelta, ClusterMetadata) =
    val id = CoordinatorShard.group(group)
    val before = CoordinatorShardState.payloads(base.coordinator.groupState, base.coordinator.deliveryState)
    val offsets = GroupCodec.decode(base.coordinator.groupState.toArray).offsets.filterNot(_.key.groupId == group) :+
      OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, 1L))
    val groups = GroupCodec.encode(GroupImage(0L, Vector.empty, offsets)).toVector
    val payload = GroupShardCodec.split(groups)(id)
    val delta = MetadataDelta(base.version, base.coordinator.version,
      CoordinatorDelta(base.controllerTerm, Vector(CoordinatorShardUpdate(id, base.coordinator.shardVersion(id), payload))))
    require(before.size == CoordinatorShard.Count)
    delta -> delta.applyTo(base).toOption.get

final class MetadataDeltaSuite extends FunSuite:
  import MetadataDeltaFixture.*

  test("coordinator-only deltas preserve cluster structure and exact shard versions") {
    val (delta, next) = update(base)
    assertEquals(MetadataDelta.between(base, next), Some(delta))
    assertEquals(delta.applyTo(base), Right(next))
    assertEquals(next.version, base.version + 1L)
    assertEquals(next.topics, base.topics)
  }

  test("replay rejects wrong metadata coordinator term and shard bases") {
    val (delta, next) = update(base)
    assert(delta.applyTo(next).isLeft)
    assert(delta.applyTo(base.copy(controllerTerm = 5L)).isLeft)
    assert(delta.applyTo(base.copy(coordinator = base.coordinator.copy(version = 1L))).isLeft)
    assert(delta.copy(change = delta.change.copy(updates = delta.change.updates.map(_.copy(expectedVersion = 5L)))).applyTo(base).isLeft)
    assert(delta.applyTo(base.copy(featureLevels = Map.empty)).isLeft)
  }

  test("structural changes feature transitions and skipped versions require snapshots") {
    val (_, next) = update(base)
    assert(MetadataDelta.between(base, next.copy(version = next.version + 1L)).isEmpty)
    assert(MetadataDelta.between(base, next.copy(controllerTerm = 5L)).isEmpty)
    assert(MetadataDelta.between(base, next.copy(unavailableBrokerIds = Set(2))).isEmpty)
    assert(MetadataDelta.between(base.copy(featureLevels = Map.empty), next).isEmpty)
    assert(MetadataDelta.between(base, base.copy(version = base.version + 1L)).isEmpty)
  }

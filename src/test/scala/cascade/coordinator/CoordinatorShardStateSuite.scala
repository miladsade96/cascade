package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
import cascade.protocol.ByteWriter
import munit.FunSuite

final class CoordinatorShardStateSuite extends FunSuite:
  private val empty = CoordinatorMetadata.Empty
  private val shards = CoordinatorShardState.payloads(Vector.empty, Vector.empty)
  private def delta(ids: Int*): CoordinatorDelta =
    CoordinatorDelta(7L, ids.toVector.map(id => CoordinatorShardUpdate(id, 0L, shards(id))))

  test("unrelated shards commit from the same base without overwriting each other") {
    val first = CoordinatorShardState.merge(empty, delta(1), 7L).toOption.get
    val second = CoordinatorShardState.merge(first, delta(2), 7L).toOption.get
    assertEquals(second.version, 2L)
    assertEquals(second.shardVersion(1), 1L)
    assertEquals(second.shardVersion(2), 1L)
    assertEquals(second.shardVersion(3), 0L)
  }
  test("one stale shard rejects the entire multi-shard transaction") {
    val first = CoordinatorShardState.merge(empty, delta(1), 7L).toOption.get
    assert(CoordinatorShardState.merge(first, delta(1, 2), 7L).isLeft)
    assertEquals(first.shardVersion(2), 0L)
  }
  test("stale controller terms are rejected even when versions match") {
    assert(CoordinatorShardState.merge(empty, delta(1), 8L).isLeft)
  }
  test("allocator version fences concurrent reservations and rejects counter rollback") {
    val allocation = CoordinatorShardUpdate(CoordinatorShard.Allocator, 0L, ByteWriter().writeLong(10L).result().toVector)
    val first = CoordinatorShardState.merge(empty, CoordinatorDelta(7L, Vector(allocation)), 7L).toOption.get
    assert(CoordinatorShardState.merge(first, CoordinatorDelta(7L, Vector(allocation)), 7L).isLeft)
    val backward = allocation.copy(expectedVersion = 1L, payload = ByteWriter().writeLong(1L).result().toVector)
    assert(CoordinatorShardState.merge(first, CoordinatorDelta(7L, Vector(backward)), 7L).isLeft)
  }
  test("no-op snapshots produce no network delta and malformed payloads fail closed") {
    assertEquals(CoordinatorShardState.changes(empty, shards, shards, 7L), None)
    val bad = CoordinatorDelta(7L, Vector(CoordinatorShardUpdate(1, 0L, Vector.empty)))
    assert(CoordinatorShardState.merge(empty, bad, 7L).isLeft)
  }

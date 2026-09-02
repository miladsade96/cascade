package cascade.coordinator

import munit.FunSuite

final class CoordinatorShardSuite extends FunSuite:
  test("layout is deterministic, domain separated, and bounded") {
    assertEquals(CoordinatorShard.group("orders"), 28)
    assertEquals(CoordinatorShard.transaction("orders"), 92)
    assertEquals(CoordinatorShard.producer(1L, Some("orders")), 92)
    assertEquals(CoordinatorShard.Count, 129)
    assert(CoordinatorShard.valid(CoordinatorShard.Allocator))
    assert(!CoordinatorShard.valid(-1))
    assert(!CoordinatorShard.valid(129))
  }

  test("high-cardinality IDs use every virtual shard") {
    val counts = (0 until 10000).map(i => CoordinatorShard.group(s"group-$i")).groupMapReduce(identity)(_ => 1)(_ + _)
    assertEquals(counts.size, 64)
    assert(counts.values.forall(count => count > 90 && count < 220), counts)
    assert((0L until 10000L).forall(id => CoordinatorShard.producer(id, None) >= 64))
  }

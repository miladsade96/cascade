package cascade.cluster

import cascade.coordinator.CoordinatorShard
import munit.FunSuite

final class CoordinatorPayloadCacheSuite extends FunSuite:
  test("shard views are image-local immutable caches and copies cannot retain stale payloads") {
    val before = MetadataDeltaFixture.update(MetadataDeltaFixture.base, "workers", 1L)._2
    val cached = before.coordinator.shardPayloads
    assert(cached eq before.coordinator.shardPayloads)
    val after = MetadataDeltaFixture.update(before, "workers", 2L)._2
    assertNotEquals(cached(CoordinatorShard.group("workers")), after.coordinator.shardPayloads(CoordinatorShard.group("workers")))
    assertEquals(before.coordinator.shardPayloads, cached)
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(after)).coordinator.shardPayloads, after.coordinator.shardPayloads)
  }

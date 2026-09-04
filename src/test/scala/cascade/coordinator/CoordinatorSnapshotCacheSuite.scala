package cascade.coordinator

import cascade.group.*
import cascade.delivery.*
import munit.FunSuite

final class CoordinatorSnapshotCacheSuite extends FunSuite:
  test("a combined snapshot remains exactly equivalent to the legacy full-state split") {
    val cache = CoordinatorSnapshotCache()
    val groups = GroupImage(9L, Vector.empty, Vector(OffsetCommitValue(GroupOffsetKey("workers", "events", 0), CommittedOffset(7L, 1, None, 42L))))
    val delivery = DeliveryImage(12L, 2L, Vector(ProducerRegistration(1L, 0, Some("orders"), 10000)), Vector.empty, Vector.empty)
    val first = cache.capture(groups, delivery)
    assertEquals(first.payloads, CoordinatorShardState.payloads(GroupCodec.encode(groups).toVector, DeliveryCodec.encode(delivery).toVector))
    assertEquals(first.fullImageBytes, GroupCodec.encode(groups).length.toLong + DeliveryCodec.encode(delivery).length)
    assertEquals(first.encoded + first.reused, CoordinatorShard.Count)
    val repeated = cache.capture(groups, delivery)
    assertEquals(repeated.encoded, 0)
    assertEquals(repeated.reused, 129)
    assertEquals(repeated.encodedBytes, 0L)
  }

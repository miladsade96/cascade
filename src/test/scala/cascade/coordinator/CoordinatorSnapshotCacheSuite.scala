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

  test("random mixed updates deletion and rollback match uncached encoding across 500 transitions") {
    val random = scala.util.Random(42L)
    val cache = CoordinatorSnapshotCache()
    var groups = GroupImage.Empty
    var delivery = DeliveryImage.Empty
    (0 until 500).foreach { i =>
      val key = GroupOffsetKey(s"g-${random.nextInt(100)}", "events", random.nextInt(4))
      val values = groups.offsets.filterNot(_.key == key)
      groups = groups.copy(version = i.toLong, offsets =
        if i % 7 == 0 then values
        else values :+ OffsetCommitValue(key, CommittedOffset(random.nextInt(100).toLong, -1, Some(s"meta-$i"), i.toLong)))
      if i % 3 == 0 then
        val id = delivery.nextProducerId
        delivery = delivery.copy(nextProducerId = id + 1L, producers = delivery.producers :+ ProducerRegistration(id, 0, None, 10000))
      if i % 29 == 0 then
        groups = GroupImage.Empty
        delivery = DeliveryImage.Empty
      val snapshot = cache.capture(groups, delivery)
      assertEquals(snapshot.payloads, CoordinatorShardState.payloads(GroupCodec.encode(groups).toVector, DeliveryCodec.encode(delivery).toVector))
      assertEquals(snapshot.fullImageBytes, GroupCodec.encode(groups).length.toLong + DeliveryCodec.encode(delivery).length)
      assertEquals(snapshot.encoded + snapshot.reused, 129)
    }
  }

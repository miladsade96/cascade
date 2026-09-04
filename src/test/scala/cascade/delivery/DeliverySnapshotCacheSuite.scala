package cascade.delivery

import munit.FunSuite

final class DeliverySnapshotCacheSuite extends FunSuite:
  test("unchanged delivery state reuses all buckets including the allocator") {
    val cache = DeliverySnapshotCache()
    val first = cache.capture(DeliveryImage.Empty)
    assertEquals(first.encoded, 65)
    val again = cache.capture(DeliveryImage.Empty)
    assertEquals(again.reused, 65)
    assert(first.payloads eq again.payloads)
    assertEquals(cache.capture(DeliveryImage.Empty.copy(version = 12L)).encoded, 0)
  }

  test("producer allocation changes exactly one transaction bucket and the allocator") {
    val cache = DeliverySnapshotCache()
    cache.capture(DeliveryImage.Empty)
    val image = DeliveryImage(0L, 2L, Vector(ProducerRegistration(1L, 0, Some("orders"), 30000)), Vector.empty, Vector.empty)
    val captured = cache.capture(image)
    assertEquals(captured.encoded, 2)
    assertEquals(captured.payloads, DeliveryShardCodec.split(image))
    assertEquals(DeliverySnapshotCache.fullImageBytes(captured.payloads), DeliveryCodec.encode(image).length.toLong)
  }

  test("provisional transaction state with an unchanged image version invalidates cached bytes") {
    val cache = DeliverySnapshotCache()
    val before = DeliveryImage.Empty
    cache.capture(before)
    val active = ActiveTransaction("orders", 1L, 0, 30000, 42L, Vector.empty, Vector(TransactionRange("events", 0, 0L, 1L)), Vector.empty, Vector.empty)
    val after = before.copy(activeTransactions = Vector(active))
    assertEquals(after.version, before.version)
    assertEquals(cache.capture(after).encoded, 1)
    assertEquals(cache.capture(before).payloads, DeliveryShardCodec.split(before))
  }

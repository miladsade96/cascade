package cascade.delivery

import cascade.coordinator.CoordinatorShard
import cascade.protocol.ByteWriter
import munit.FunSuite

final class DeliveryShardCodecSuite extends FunSuite:
  private val producer = ProducerRegistration(1L, 0, Some("orders"), 10000)
  private val active = ActiveTransaction("orders", 1L, 0, 10000, 1L, Vector.empty, Vector.empty, Vector("workers"), Vector.empty)
  private val image = DeliveryImage(4L, 2L, Vector(producer), Vector(active), Vector.empty)

  test("typed delivery partitions retain byte identity and normalize the separate allocator") {
    assertEquals(DeliveryShardCodec.split(image), DeliveryShardCodec.split(DeliveryCodec.encode(image).toVector))
    assertEquals(DeliveryShardCodec.partition(image).map(p => (p.version, p.nextProducerId)).distinct, Vector((0L, 1L)))
    assertEquals(DeliveryShardCodec.split(DeliveryImage.Empty), DeliveryShardCodec.split(Vector.empty[Byte]))
  }

  test("transaction and producer registration share a shard with a separate allocator") {
    val shards = DeliveryShardCodec.split(DeliveryCodec.encode(image).toVector)
    assertEquals(DeliveryCodec.decode(DeliveryShardCodec.merge(shards, 5L).toArray), image.copy(version = 5L))
    val payload = DeliveryCodec.decode(shards(CoordinatorShard.transaction("orders") - 64).toArray)
    assertEquals(payload.producers, Vector(producer))
    assertEquals(payload.activeTransactions, Vector(active))
  }

  test("allocation counter cannot admit duplicate or out-of-range producer IDs") {
    val shards = DeliveryShardCodec.split(DeliveryCodec.encode(image).toVector)
    intercept[IllegalArgumentException](DeliveryShardCodec.merge(shards.updated(64, ByteWriter().writeLong(1L).result().toVector), 5L))
    val duplicate = image.copy(producers = Vector(producer, producer))
    intercept[IllegalArgumentException](DeliveryShardCodec.merge(DeliveryShardCodec.split(DeliveryCodec.encode(duplicate).toVector), 5L))
  }

  test("a transaction transition leaves allocator and unrelated shards unchanged") {
    val before = DeliveryShardCodec.split(DeliveryCodec.encode(image).toVector)
    val after = DeliveryShardCodec.split(DeliveryCodec.encode(image.copy(activeTransactions = Vector.empty)).toVector)
    assertEquals(before.zip(after).count((a, b) => a != b), 1)
    assertEquals(before.last, after.last)
  }

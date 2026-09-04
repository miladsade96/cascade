package cascade.delivery

import cascade.coordinator.{CoordinatorShard, EncodedShards, ShardEncodingCache}
import cascade.protocol.ByteWriter

private[cascade] final class DeliverySnapshotCache:
  private val transactions = ShardEncodingCache[DeliveryImage](CoordinatorShard.Buckets, value => DeliveryCodec.encode(value).toVector)
  private val allocator = ShardEncodingCache[Long](1, value => ByteWriter().writeLong(value).result().toVector)
  private var previous: Option[(DeliveryImage, Vector[Vector[Byte]])] = None

  def capture(image: DeliveryImage): EncodedShards = synchronized {
    previous match
      case Some((before, payloads)) if before eq image => EncodedShards(payloads, 0, CoordinatorShard.Buckets + 1, 0L)
      case _ =>
        val data = transactions.capture(DeliveryShardCodec.partition(image))
        val counter = allocator.capture(Vector(image.nextProducerId))
        val payloads = data.payloads ++ counter.payloads
        previous = Some(image -> payloads)
        EncodedShards(payloads, data.encoded + counter.encoded, data.reused + counter.reused, data.encodedBytes + counter.encodedBytes)
  }

private[cascade] object DeliverySnapshotCache:
  def fullImageBytes(payloads: Vector[Vector[Byte]]): Long =
    require(payloads.size == CoordinatorShard.Buckets + 1, "invalid delivery payload count")
    28L + payloads.init.iterator.map(_.size.toLong - 28L).sum

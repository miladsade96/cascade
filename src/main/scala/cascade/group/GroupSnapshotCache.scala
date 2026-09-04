package cascade.group

import cascade.coordinator.{CoordinatorShard, EncodedShards, ShardEncodingCache}

private[cascade] final class GroupSnapshotCache:
  private val cache = ShardEncodingCache[GroupImage](CoordinatorShard.Buckets, value => GroupCodec.encode(value).toVector)

  def capture(image: GroupImage): EncodedShards = cache.capture(GroupShardCodec.partition(image))

private[cascade] object GroupSnapshotCache:
  /** Exact legacy full-image size without allocating that image. Format 2 adds one array count. */
  def fullImageBytes(payloads: Vector[Vector[Byte]]): Long =
    require(payloads.size == CoordinatorShard.Buckets, "invalid group payload count")
    def header(bytes: Vector[Byte]): Int = if bytes(1) == 2 then 22 else 18
    val body = payloads.iterator.map(bytes => bytes.size.toLong - header(bytes)).sum
    body + (if payloads.exists(bytes => bytes(1) == 2) then 22L else 18L)

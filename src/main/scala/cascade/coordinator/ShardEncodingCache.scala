package cascade.coordinator

/** One immutable input and payload per fixed shard; no key- or version-history retention. */
private[cascade] final class ShardEncodingCache[A](size: Int, encode: A => Vector[Byte]):
  require(size > 0 && size <= CoordinatorShard.Count, "invalid encoding cache size")
  private var previous = Vector.empty[A]
  private var payloads = Vector.empty[Vector[Byte]]

  def capture(values: Vector[A]): EncodedShards = synchronized {
    require(values.size == size, "invalid encoding cache input count")
    var encoded = 0
    var encodedBytes = 0L
    // Publish the replacement only after every encode succeeds. A rejected candidate
    // may reuse the same version later with different contents.
    val next = values.indices.map { id =>
      if previous.nonEmpty && previous(id) == values(id) then payloads(id)
      else
        val bytes = encode(values(id))
        encoded += 1
        encodedBytes += bytes.size.toLong
        bytes
    }.toVector
    previous = values
    payloads = next
    EncodedShards(next, encoded, size - encoded, encodedBytes)
  }

private[cascade] final case class EncodedShards(
    payloads: Vector[Vector[Byte]], encoded: Int, reused: Int, encodedBytes: Long
)

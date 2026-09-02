package cascade.cluster

import cascade.coordinator.CoordinatorShard
import cascade.protocol.{ByteCursor, ByteWriter}
import java.security.MessageDigest

/** The shard namespace is part of the digest, preventing cross-shard substitution. */
final case class ShardObjectRef(shard: Int, length: Int, digest: Vector[Byte]):
  require(shard >= 0 && shard <= ShardObjectRef.DeliverySnapshot, "invalid shard object namespace")
  require(length >= 0 && length <= ShardObjectRef.MaximumBytes, "invalid shard object length")
  require(digest.size == 32, "invalid shard object digest")
  def fileName: String = s"$shard-${digest.map(b => f"${b & 0xff}%02x").mkString}.shard"

object ShardObjectRef:
  val GroupSnapshot = CoordinatorShard.Count
  val DeliverySnapshot = CoordinatorShard.Count + 1
  val MaximumBytes = 64 * 1024 * 1024

  def identify(shard: Int, bytes: Array[Byte]): ShardObjectRef =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(ByteWriter().writeInt(shard).result())
    ShardObjectRef(shard, bytes.length, digest.digest(bytes).toVector)

  def write(writer: ByteWriter, ref: ShardObjectRef): Unit =
    writer.writeInt(ref.shard).writeInt(ref.length).writeBytes(ref.digest.toArray): Unit

  def read(cursor: ByteCursor): ShardObjectRef =
    ShardObjectRef(cursor.readInt(), cursor.readInt(), cursor.readBytes(32).toVector)

final case class ShardObjectSnapshot(
    writtenBytes: Long = 0L,
    writtenObjects: Long = 0L,
    reusedObjects: Long = 0L,
    reclaimedBytes: Long = 0L,
    reclaimedObjects: Long = 0L,
    liveBytes: Long = 0L,
    directoryForceSupported: Boolean = false
)

package cascade.coordinator

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

final case class CoordinatorShardUpdate(id: Int, expectedVersion: Long, payload: Vector[Byte]):
  require(CoordinatorShard.valid(id), "invalid coordinator shard ID")
  require(expectedVersion >= 0L, "negative coordinator shard version")

final case class CoordinatorDelta(controllerTerm: Long, updates: Vector[CoordinatorShardUpdate]):
  require(controllerTerm >= 0L, "negative coordinator controller term")
  require(updates.nonEmpty && updates.size <= CoordinatorShard.Count, "invalid coordinator delta size")
  require(updates.map(_.id).distinct.size == updates.size, "duplicate coordinator shard update")

object CoordinatorDeltaCodec:
  def encode(delta: CoordinatorDelta): Array[Byte] =
    val writer = ByteWriter().writeShort(1).writeLong(delta.controllerTerm)
    writer.writeArray(delta.updates.sortBy(_.id)) { update =>
      writer.writeInt(update.id).writeLong(update.expectedVersion).writeByteArray(update.payload.toArray): Unit
    }
    writer.result()

  def decode(cursor: ByteCursor): CoordinatorDelta =
    if cursor.readShort() != 1 then throw ProtocolException("unsupported coordinator delta format")
    val term = cursor.readLong()
    val count = cursor.readInt()
    if count <= 0 || count > CoordinatorShard.Count then throw ProtocolException("invalid coordinator delta count")
    val updates = Vector.fill(count)(CoordinatorShardUpdate(cursor.readInt(), cursor.readLong(), cursor.readByteArray().toVector))
    cursor.ensureFullyRead()
    CoordinatorDelta(term, updates)

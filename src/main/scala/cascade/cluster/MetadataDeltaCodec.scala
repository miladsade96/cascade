package cascade.cluster

import cascade.coordinator.CoordinatorDeltaCodec
import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

/** Negative discriminator cannot be mistaken for any historical full-image format. */
object MetadataDeltaCodec:
  val RecordFormat: Short = -10
  val MaximumBytes = 64 * 1024 * 1024

  def encode(delta: MetadataDelta): Array[Byte] =
    val size = 32L + delta.change.updates.iterator.map(update => 16L + update.payload.size).sum
    if size > MaximumBytes then throw ProtocolException("metadata delta exceeds the journal frame limit")
    ByteWriter().writeShort(RecordFormat).writeLong(delta.baseVersion).writeLong(delta.baseCoordinatorVersion)
      .writeBytes(CoordinatorDeltaCodec.encode(delta.change)).result()

  def decode(bytes: Array[Byte]): MetadataDelta =
    if bytes.length > MaximumBytes then throw ProtocolException("metadata delta exceeds the journal frame limit")
    val cursor = ByteCursor(bytes)
    if cursor.readShort() != RecordFormat then throw ProtocolException("unsupported metadata delta record format")
    MetadataDelta(cursor.readLong(), cursor.readLong(), CoordinatorDeltaCodec.decode(cursor))

  def isDelta(bytes: Array[Byte]): Boolean = ByteCursor(bytes).readShort() == RecordFormat

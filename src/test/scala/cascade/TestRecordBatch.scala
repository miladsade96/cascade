package cascade

import java.nio.{ByteBuffer, ByteOrder}
import java.io.ByteArrayOutputStream
import java.util.zip.{CRC32C, GZIPOutputStream}

object TestRecordBatch:
  final case class Record(key: Option[Array[Byte]], value: Option[Array[Byte]], timestamp: Long)

  def single(offset: Long = 0L, totalBytes: Int = 61): Array[Byte] =
    require(totalBytes >= 61, "record batch must contain the complete magic-v2 header")
    val buffer = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(offset)
    buffer.putInt(totalBytes - 12) // bytes after batchLength
    buffer.putInt(0) // partition leader epoch
    buffer.put(2.toByte)
    buffer.putInt(0) // CRC is opaque to storage tests
    buffer.putShort(0.toShort)
    buffer.putInt(0) // last offset delta
    buffer.putLong(0L)
    buffer.putLong(0L)
    buffer.putLong(-1L)
    buffer.putShort(-1.toShort)
    buffer.putInt(-1)
    buffer.putInt(1)
    buffer.array()

  def producer(
      producerId: Long,
      producerEpoch: Short,
      baseSequence: Int,
      transactional: Boolean = false,
      recordCount: Int = 1,
      totalBytes: Int = 61
  ): Array[Byte] =
    require(recordCount > 0, "producer batch must contain records")
    val batch = single(totalBytes = totalBytes)
    val buffer = ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN)
    buffer.putShort(21, (if transactional then 0x10 else 0).toShort)
    buffer.putInt(23, recordCount - 1)
    buffer.putLong(43, producerId)
    buffer.putShort(51, producerEpoch)
    buffer.putInt(53, baseSequence)
    buffer.putInt(57, recordCount)
    batch

  def keyed(records: Vector[Record], baseOffset: Long = 0L): Array[Byte] =
    require(records.nonEmpty, "a keyed batch needs at least one record")
    val baseTimestamp = records.head.timestamp
    val encoded = records.zipWithIndex.flatMap { case (record, offsetDelta) =>
      val body = Vector.newBuilder[Byte]
      body += 0.toByte
      body ++= encodeVarLong(record.timestamp - baseTimestamp)
      body ++= encodeVarInt(offsetDelta)
      body ++= encodeNullableBytes(record.key)
      body ++= encodeNullableBytes(record.value)
      body ++= encodeVarInt(0)
      val bytes = body.result()
      encodeVarInt(bytes.length) ++ bytes
    }.toArray
    val totalBytes = 61 + encoded.length
    val buffer = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(baseOffset)
    buffer.putInt(totalBytes - 12)
    buffer.putInt(0)
    buffer.put(2.toByte)
    buffer.putInt(0)
    buffer.putShort(0.toShort)
    buffer.putInt(records.size - 1)
    buffer.putLong(baseTimestamp)
    buffer.putLong(records.map(_.timestamp).max)
    buffer.putLong(-1L)
    buffer.putShort(-1.toShort)
    buffer.putInt(-1)
    buffer.putInt(records.size)
    buffer.put(encoded)
    buffer.array()

  def gzipKeyed(records: Vector[Record], baseOffset: Long = 0L): Array[Byte] =
    val plain = keyed(records, baseOffset)
    val compressed = ByteArrayOutputStream()
    val gzip = GZIPOutputStream(compressed)
    try gzip.write(plain, 61, plain.length - 61)
    finally gzip.close()
    val payload = compressed.toByteArray
    val batch = java.util.Arrays.copyOf(plain, 61 + payload.length)
    System.arraycopy(payload, 0, batch, 61, payload.length)
    val buffer = ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN)
    buffer.putInt(8, batch.length - 12)
    buffer.putShort(21, (buffer.getShort(21) | 1).toShort)
    val crc = CRC32C()
    crc.update(batch, 21, batch.length - 21)
    buffer.putInt(17, crc.getValue.toInt)
    batch

  private def encodeNullableBytes(value: Option[Array[Byte]]): Vector[Byte] =
    value match
      case None => encodeVarInt(-1)
      case Some(bytes) => encodeVarInt(bytes.length) ++ bytes.toVector

  private def encodeVarInt(value: Int): Vector[Byte] = encodeUnsigned((value.toLong << 1) ^ (value >> 31).toLong)

  private def encodeVarLong(value: Long): Vector[Byte] = encodeUnsigned((value << 1) ^ (value >> 63))

  private def encodeUnsigned(value: Long): Vector[Byte] =
    val bytes = Vector.newBuilder[Byte]
    var remaining = value
    while (remaining & ~0x7fL) != 0L do
      bytes += ((remaining & 0x7fL) | 0x80L).toByte
      remaining >>>= 7
    bytes += remaining.toByte
    bytes.result()

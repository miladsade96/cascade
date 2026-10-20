package cascade

import java.nio.{ByteBuffer, ByteOrder}
import java.io.{ByteArrayOutputStream, OutputStream}
import java.util.zip.GZIPOutputStream
import cascade.storage.RecordBatch
import org.xerial.snappy.SnappyOutputStream
import net.jpountz.lz4.LZ4FrameOutputStream
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer

object TestRecordBatch:
  final case class Record(key: Option[Array[Byte]], value: Option[Array[Byte]], timestamp: Long)

  def single(offset: Long = 0L, totalBytes: Int = 61): Array[Byte] =
    require(totalBytes >= 61, "record batch must contain the complete magic-v2 header")
    val buffer = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(offset)
    buffer.putInt(totalBytes - 12) // bytes after batchLength
    buffer.putInt(0) // partition leader epoch
    buffer.put(2.toByte)
    buffer.putInt(0) // CRC is finalized after the complete batch is encoded
    buffer.putShort(0.toShort)
    buffer.putInt(0) // last offset delta
    buffer.putLong(0L)
    buffer.putLong(0L)
    buffer.putLong(-1L)
    buffer.putShort(-1.toShort)
    buffer.putInt(-1)
    buffer.putInt(1)
    val batch = buffer.array()
    RecordBatch.rewriteCrc(batch)
    batch

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
    RecordBatch.rewriteCrc(batch)
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
    val batch = buffer.array()
    RecordBatch.rewriteCrc(batch)
    batch

  def keyedProducer(
      records: Vector[Record],
      producerId: Long,
      producerEpoch: Short,
      baseSequence: Int,
      transactional: Boolean = false,
      control: Boolean = false,
      baseOffset: Long = 0L
  ): Array[Byte] =
    require(producerId >= 0L, "producer ID must be non-negative")
    require(producerEpoch >= 0, "producer epoch must be non-negative")
    require(baseSequence >= 0, "base sequence must be non-negative")
    val batch = keyed(records, baseOffset)
    val buffer = ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN)
    val flags = (if transactional then 0x10 else 0) | (if control then 0x20 else 0)
    buffer.putShort(21, (buffer.getShort(21) | flags).toShort)
    buffer.putLong(43, producerId)
    buffer.putShort(51, producerEpoch)
    buffer.putInt(53, baseSequence)
    RecordBatch.rewriteCrc(batch)
    batch

  def gzipKeyed(records: Vector[Record], baseOffset: Long = 0L): Array[Byte] =
    compressedKeyed(records, compressionType = 1, baseOffset = baseOffset)

  def compressedKeyed(records: Vector[Record], compressionType: Int, baseOffset: Long = 0L): Array[Byte] =
    require(compressionType >= 1 && compressionType <= 4, "compression type must be gzip, snappy, lz4, or zstd")
    val plain = keyed(records, baseOffset)
    val compressed = ByteArrayOutputStream()
    val codec: OutputStream = compressionType match
      case 1 => GZIPOutputStream(compressed)
      case 2 => SnappyOutputStream(compressed)
      case 3 => LZ4FrameOutputStream(compressed)
      case 4 => ZstdOutputStreamNoFinalizer(compressed)
    try codec.write(plain, 61, plain.length - 61)
    finally codec.close()
    val payload = compressed.toByteArray
    val batch = java.util.Arrays.copyOf(plain, 61 + payload.length)
    System.arraycopy(payload, 0, batch, 61, payload.length)
    val buffer = ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN)
    buffer.putInt(8, batch.length - 12)
    buffer.putShort(21, (buffer.getShort(21) | compressionType).toShort)
    RecordBatch.rewriteCrc(batch)
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

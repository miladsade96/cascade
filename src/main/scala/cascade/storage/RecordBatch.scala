package cascade.storage

import cascade.protocol.ProtocolException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Arrays

final case class PreparedBatch(baseOffset: Long, lastOffset: Long, bytes: Array[Byte])
final case class RecordBatchMetadata(
    baseOffset: Long,
    lastOffset: Long,
    producerId: Long,
    producerEpoch: Short,
    baseSequence: Int,
    lastSequence: Int,
    recordCount: Int,
    transactional: Boolean,
    control: Boolean
)

object RecordBatch:
  private val MinimumSize = 61

  /** Splits a Kafka record set, validates batch envelopes, and assigns broker offsets. */
  def prepare(recordSet: Array[Byte], firstOffset: Long): Vector[PreparedBatch] =
    val builder = Vector.newBuilder[PreparedBatch]
    var position = 0
    var nextOffset = firstOffset
    while position < recordSet.length do
      if recordSet.length - position < 12 then
        throw ProtocolException("truncated record batch envelope")
      val view = ByteBuffer.wrap(recordSet, position, recordSet.length - position).order(ByteOrder.BIG_ENDIAN)
      view.getLong()
      val batchLength = view.getInt()
      val totalSize = Math.addExact(batchLength, 12)
      if totalSize < MinimumSize || totalSize > recordSet.length - position then
        throw ProtocolException(s"invalid record batch length: $batchLength")
      val batch = Arrays.copyOfRange(recordSet, position, position + totalSize)
      val header = ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN)
      if header.get(16) != 2.toByte then throw ProtocolException("only Kafka record batch magic 2 is supported")
      val lastOffsetDelta = header.getInt(23)
      val recordCount = header.getInt(57)
      if lastOffsetDelta < 0 || recordCount < 0 then throw ProtocolException("negative record batch counters")
      header.putLong(0, nextOffset)
      val lastOffset = Math.addExact(nextOffset, lastOffsetDelta.toLong)
      builder += PreparedBatch(nextOffset, lastOffset, batch)
      nextOffset = Math.addExact(lastOffset, 1L)
      position += totalSize
    builder.result()

  def baseOffset(bytes: Array[Byte]): Long =
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong(0)

  def lastOffset(bytes: Array[Byte]): Long =
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    Math.addExact(buffer.getLong(0), buffer.getInt(23).toLong)

  def metadata(bytes: Array[Byte]): RecordBatchMetadata =
    if bytes.length < MinimumSize then throw ProtocolException("record batch is too short")
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    if buffer.get(16) != 2.toByte then throw ProtocolException("only Kafka record batch magic 2 is supported")
    val base = buffer.getLong(0)
    val last = Math.addExact(base, buffer.getInt(23).toLong)
    val producerId = buffer.getLong(43)
    val producerEpoch = buffer.getShort(51)
    val baseSequence = buffer.getInt(53)
    val recordCount = buffer.getInt(57)
    val attributes = buffer.getShort(21).toInt & 0xffff
    val lastSequence =
      if baseSequence < 0 || recordCount == 0 then baseSequence
      else ((baseSequence.toLong + recordCount.toLong - 1L) % (Int.MaxValue.toLong + 1L)).toInt
    RecordBatchMetadata(
      base,
      last,
      producerId,
      producerEpoch,
      baseSequence,
      lastSequence,
      recordCount,
      transactional = (attributes & 0x10) != 0,
      control = (attributes & 0x20) != 0
    )

  def totalSize(prefix: Array[Byte]): Int =
    if prefix.length < 12 then throw ProtocolException("record batch prefix is too short")
    Math.addExact(ByteBuffer.wrap(prefix).order(ByteOrder.BIG_ENDIAN).getInt(8), 12)

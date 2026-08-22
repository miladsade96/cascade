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
    control: Boolean,
    maxTimestamp: Long,
    compressionType: Int
)

final case class IndexedRecord(offset: Long, timestamp: Long, key: Option[Vector[Byte]], tombstone: Boolean)

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
      control = (attributes & 0x20) != 0,
      maxTimestamp = buffer.getLong(35),
      compressionType = attributes & 0x07
    )

  /** Decodes keys from an uncompressed magic-v2 batch; compressed or malformed batches stay opaque. */
  def indexedRecords(bytes: Array[Byte]): Option[Vector[IndexedRecord]] =
    try
      val metadata = RecordBatch.metadata(bytes)
      if metadata.compressionType != 0 then None
      else
        val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
        buffer.position(MinimumSize)
        val baseTimestamp = buffer.getLong(27)
        val records = Vector.newBuilder[IndexedRecord]
        var count = 0
        while count < metadata.recordCount do
          val recordLength = readVarInt(buffer)
          if recordLength < 0 || recordLength > buffer.remaining() then
            throw ProtocolException(s"invalid record length: $recordLength")
          val recordEnd = Math.addExact(buffer.position(), recordLength)
          buffer.get()
          val timestampDelta = readVarLong(buffer)
          val offsetDelta = readVarInt(buffer)
          if offsetDelta < 0 then throw ProtocolException(s"negative record offset delta: $offsetDelta")
          val key = readNullableBytes(buffer)
          val value = readNullableBytes(buffer)
          val headers = readVarInt(buffer)
          if headers < 0 then throw ProtocolException(s"negative record header count: $headers")
          var header = 0
          while header < headers do
            val headerKeyLength = readVarInt(buffer)
            if headerKeyLength < 0 || headerKeyLength > buffer.remaining() then
              throw ProtocolException(s"invalid record header key length: $headerKeyLength")
            buffer.position(buffer.position() + headerKeyLength)
            readNullableBytes(buffer)
            header += 1
          if buffer.position() != recordEnd then throw ProtocolException("record length does not match its contents")
          records += IndexedRecord(
            Math.addExact(metadata.baseOffset, offsetDelta.toLong),
            Math.addExact(baseTimestamp, timestampDelta),
            key,
            value.isEmpty
          )
          count += 1
        if buffer.hasRemaining then throw ProtocolException("record batch contains trailing bytes")
        Some(records.result())
    catch case _: Throwable => None

  def totalSize(prefix: Array[Byte]): Int =
    if prefix.length < 12 then throw ProtocolException("record batch prefix is too short")
    Math.addExact(ByteBuffer.wrap(prefix).order(ByteOrder.BIG_ENDIAN).getInt(8), 12)

  private def readNullableBytes(buffer: ByteBuffer): Option[Vector[Byte]] =
    val length = readVarInt(buffer)
    if length == -1 then None
    else if length < -1 || length > buffer.remaining() then throw ProtocolException(s"invalid byte-array length: $length")
    else
      val bytes = new Array[Byte](length)
      buffer.get(bytes)
      Some(bytes.toVector)

  private def readVarInt(buffer: ByteBuffer): Int =
    val raw = readUnsignedVarLong(buffer, 5)
    ((raw >>> 1) ^ -(raw & 1L)).toInt

  private def readVarLong(buffer: ByteBuffer): Long =
    val raw = readUnsignedVarLong(buffer, 10)
    (raw >>> 1) ^ -(raw & 1L)

  private def readUnsignedVarLong(buffer: ByteBuffer, maximumBytes: Int): Long =
    var result = 0L
    var shift = 0
    var count = 0
    var complete = false
    while !complete && count < maximumBytes do
      if !buffer.hasRemaining then throw ProtocolException("truncated variable-length integer")
      val value = buffer.get().toInt & 0xff
      result |= (value & 0x7f).toLong << shift
      complete = (value & 0x80) == 0
      shift += 7
      count += 1
    if !complete then throw ProtocolException("variable-length integer is too long")
    result

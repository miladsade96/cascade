package cascade.storage

import cascade.protocol.CorruptRecordException
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.Arrays
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.{CRC32C, GZIPInputStream, GZIPOutputStream}
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}
import net.jpountz.lz4.{LZ4FrameInputStream, LZ4FrameOutputStream}
import com.github.luben.zstd.{ZstdInputStreamNoFinalizer, ZstdOutputStreamNoFinalizer}

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
private final case class EncodedRecord(indexed: IndexedRecord, bytes: Array[Byte])

object RecordBatch:
  private val MinimumSize = 61
  private val CrcOffset = 17
  private val CrcCoverageOffset = 21
  private val MaximumDecodedBatchBytes = 128 * 1024 * 1024

  private[cascade] def computedCrc(bytes: Array[Byte]): Int =
    if bytes.length < CrcCoverageOffset then throw CorruptRecordException("record batch is too short for CRC32C")
    val crc = CRC32C()
    crc.update(bytes, CrcCoverageOffset, bytes.length - CrcCoverageOffset)
    crc.getValue.toInt

  private[cascade] def storedCrc(bytes: Array[Byte]): Int =
    if bytes.length < CrcCoverageOffset then throw CorruptRecordException("record batch is too short for CRC32C")
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(CrcOffset)

  private[cascade] def rewriteCrc(bytes: Array[Byte]): Unit =
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).putInt(CrcOffset, computedCrc(bytes)): Unit

  private[cascade] def hasValidCrc(bytes: Array[Byte]): Boolean =
    storedCrc(bytes) == computedCrc(bytes)

  /** Splits a Kafka record set, validates batch envelopes, and assigns broker offsets. */
  def prepare(recordSet: Array[Byte], firstOffset: Long): Vector[PreparedBatch] =
    val builder = Vector.newBuilder[PreparedBatch]
    var position = 0
    var nextOffset = firstOffset
    while position < recordSet.length do
      if recordSet.length - position < 12 then
        throw CorruptRecordException("truncated record batch envelope")
      val view = ByteBuffer.wrap(recordSet, position, recordSet.length - position).order(ByteOrder.BIG_ENDIAN)
      view.getLong()
      val batchLength = view.getInt()
      val totalSize =
        try Math.addExact(batchLength, 12)
        catch case _: ArithmeticException => throw CorruptRecordException(s"invalid record batch length: $batchLength")
      if totalSize < MinimumSize || totalSize > recordSet.length - position then
        throw CorruptRecordException(s"invalid record batch length: $batchLength")
      val batch = Arrays.copyOfRange(recordSet, position, position + totalSize)
      val header = validatedHeader(batch)
      val lastOffsetDelta = header.getInt(23)
      header.putLong(0, nextOffset)
      val lastOffset = Math.addExact(nextOffset, lastOffsetDelta.toLong)
      builder += PreparedBatch(nextOffset, lastOffset, batch)
      nextOffset = Math.addExact(lastOffset, 1L)
      position += totalSize
    builder.result()

  def validate(bytes: Array[Byte]): Unit =
    validatedHeader(bytes): Unit

  def baseOffset(bytes: Array[Byte]): Long =
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong(0)

  def lastOffset(bytes: Array[Byte]): Long =
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    Math.addExact(buffer.getLong(0), buffer.getInt(23).toLong)

  def metadata(bytes: Array[Byte]): RecordBatchMetadata =
    val buffer = validatedHeader(bytes)
    val base = buffer.getLong(0)
    val lastOffsetDelta = buffer.getInt(23)
    val last = Math.addExact(base, lastOffsetDelta.toLong)
    val producerId = buffer.getLong(43)
    val producerEpoch = buffer.getShort(51)
    val baseSequence = buffer.getInt(53)
    val recordCount = buffer.getInt(57)
    val attributes = buffer.getShort(21).toInt & 0xffff
    val lastSequence =
      if baseSequence < 0 || recordCount == 0 then baseSequence
      else ((baseSequence.toLong + lastOffsetDelta.toLong) % (Int.MaxValue.toLong + 1L)).toInt
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

  private def validatedHeader(bytes: Array[Byte]): ByteBuffer =
    if bytes.length < MinimumSize then throw CorruptRecordException("record batch is too short")
    val buffer = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN)
    val batchLength = buffer.getInt(8)
    val declaredSize =
      try Math.addExact(batchLength, 12)
      catch case _: ArithmeticException => throw CorruptRecordException(s"invalid record batch length: $batchLength")
    if batchLength < MinimumSize - 12 || declaredSize != bytes.length then
      throw CorruptRecordException(s"record batch length $batchLength does not match ${bytes.length - 12}")
    if buffer.get(16) != 2.toByte then throw CorruptRecordException("only Kafka record batch magic 2 is supported")
    val lastOffsetDelta = buffer.getInt(23)
    val recordCount = buffer.getInt(57)
    if lastOffsetDelta < 0 || recordCount < 0 then throw CorruptRecordException("negative record batch counters")
    val compressionType = buffer.getShort(21).toInt & 0x07
    if compressionType > 4 then throw CorruptRecordException(s"unsupported record compression type: $compressionType")
    val expectedCrc = storedCrc(bytes)
    val actualCrc = computedCrc(bytes)
    if expectedCrc != actualCrc then
      throw CorruptRecordException(
        f"record batch CRC32C mismatch: stored=0x${expectedCrc & 0xffffffffL}%08x, computed=0x${actualCrc & 0xffffffffL}%08x"
      )
    buffer

  /** Decodes keys from every Kafka magic-v2 compression codec. */
  def indexedRecords(bytes: Array[Byte]): Option[Vector[IndexedRecord]] =
    try
      val metadata = RecordBatch.metadata(bytes)
      decodedRecords(bytes, metadata).map(_.map(_.indexed))
    catch case _: Throwable => None

  /** Returns a rewritten batch, the original batch, or None when every compactable record is obsolete. */
  def compact(
      bytes: Array[Byte],
      latestOffsets: Map[Vector[Byte], Long],
      deleteTombstonesBeforeMillis: Long
  ): Option[Array[Byte]] =
    try
      val metadata = RecordBatch.metadata(bytes)
      if metadata.transactional || metadata.control then Some(bytes)
      else decodedRecords(bytes, metadata) match
        case None => Some(bytes)
        case Some(records) =>
          val retained = records.filter { record =>
            record.indexed.key match
              case None => true
              case Some(key) =>
                latestOffsets.get(key).contains(record.indexed.offset) &&
                  (!record.indexed.tombstone || record.indexed.timestamp >= deleteTombstonesBeforeMillis)
          }
          if retained.size == records.size then Some(bytes)
          else if retained.isEmpty then None
          else Some(rebuild(bytes, metadata, retained))
    catch case _: Throwable => Some(bytes)

  def totalSize(prefix: Array[Byte]): Int =
    if prefix.length < 12 then throw CorruptRecordException("record batch prefix is too short")
    val batchLength = ByteBuffer.wrap(prefix).order(ByteOrder.BIG_ENDIAN).getInt(8)
    try Math.addExact(batchLength, 12)
    catch case _: ArithmeticException => throw CorruptRecordException(s"invalid record batch length: $batchLength")

  private def decodedRecords(bytes: Array[Byte], metadata: RecordBatchMetadata): Option[Vector[EncodedRecord]] =
    val payload = Arrays.copyOfRange(bytes, MinimumSize, bytes.length)
    val decoded = metadata.compressionType match
      case 0 => payload
      case codec if codec >= 1 && codec <= 4 => decompress(payload, codec)
      case _ => return None
    val buffer = ByteBuffer.wrap(decoded).order(ByteOrder.BIG_ENDIAN)
    val baseTimestamp = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong(27)
    val lastOffsetDelta = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(23)
    if metadata.recordCount > decoded.length then
      throw CorruptRecordException("record count exceeds the decoded record payload")
    val records = Vector.newBuilder[EncodedRecord]
    var count = 0
    var previousOffsetDelta = -1
    while count < metadata.recordCount do
      val recordStart = buffer.position()
      val recordLength = readVarInt(buffer)
      if recordLength < 0 || recordLength > buffer.remaining() then
        throw CorruptRecordException(s"invalid record length: $recordLength")
      val recordEnd = Math.addExact(buffer.position(), recordLength)
      val outerLimit = buffer.limit()
      buffer.limit(recordEnd)
      val (timestampDelta, offsetDelta, key, value) =
        try
          buffer.get()
          val timestamp = readVarLong(buffer)
          val offset = readVarInt(buffer)
          if offset < 0 || offset > lastOffsetDelta then
            throw CorruptRecordException(s"record offset delta $offset is outside batch range 0..$lastOffsetDelta")
          if offset <= previousOffsetDelta then
            throw CorruptRecordException(s"record offset delta $offset is not strictly increasing")
          val recordKey = readNullableBytes(buffer)
          val recordValue = readNullableBytes(buffer)
          val headers = readVarInt(buffer)
          if headers < 0 then throw CorruptRecordException(s"negative record header count: $headers")
          if headers > buffer.remaining() / 2 then
            throw CorruptRecordException(s"record header count $headers exceeds the record boundary")
          var header = 0
          while header < headers do
            val headerKeyLength = readVarInt(buffer)
            if headerKeyLength < 0 || headerKeyLength > buffer.remaining() then
              throw CorruptRecordException(s"invalid record header key length: $headerKeyLength")
            buffer.position(buffer.position() + headerKeyLength)
            readNullableBytes(buffer)
            header += 1
          (timestamp, offset, recordKey, recordValue)
        finally buffer.limit(outerLimit): Unit
      if buffer.position() != recordEnd then throw CorruptRecordException("record length does not match its contents")
      val timestamp =
        try Math.addExact(baseTimestamp, timestampDelta)
        catch case _: ArithmeticException => throw CorruptRecordException("record timestamp overflows a signed 64-bit value")
      records += EncodedRecord(
        IndexedRecord(
          Math.addExact(metadata.baseOffset, offsetDelta.toLong),
          timestamp,
          key,
          value.isEmpty
        ),
        Arrays.copyOfRange(decoded, recordStart, recordEnd)
      )
      previousOffsetDelta = offsetDelta
      count += 1
    if buffer.hasRemaining then throw CorruptRecordException("record batch contains trailing bytes")
    Some(records.result())

  private def rebuild(original: Array[Byte], metadata: RecordBatchMetadata, records: Vector[EncodedRecord]): Array[Byte] =
    val plain = records.iterator.flatMap(_.bytes).toArray
    val payload = if metadata.compressionType == 0 then plain else compress(plain, metadata.compressionType)
    val rebuilt = Arrays.copyOf(original, MinimumSize + payload.length)
    System.arraycopy(payload, 0, rebuilt, MinimumSize, payload.length)
    val buffer = ByteBuffer.wrap(rebuilt).order(ByteOrder.BIG_ENDIAN)
    buffer.putInt(8, rebuilt.length - 12)
    buffer.putInt(57, records.size)
    val crc = CRC32C()
    crc.update(rebuilt, 21, rebuilt.length - 21)
    buffer.putInt(17, crc.getValue.toInt)
    rebuilt

  private def decompress(value: Array[Byte], codec: Int): Array[Byte] =
    val source = ByteArrayInputStream(value)
    val input: InputStream = codec match
      case 1 => GZIPInputStream(source)
      case 2 => SnappyInputStream(source)
      case 3 => LZ4FrameInputStream(source)
      case 4 => ZstdInputStreamNoFinalizer(source)
      case _ => throw CorruptRecordException(s"unsupported record compression type: $codec")
    try
      val decoded = input.readNBytes(MaximumDecodedBatchBytes + 1)
      if decoded.length > MaximumDecodedBatchBytes then throw CorruptRecordException("decompressed record batch exceeds 128 MiB")
      decoded
    finally input.close()

  private def compress(value: Array[Byte], codec: Int): Array[Byte] =
    val output = ByteArrayOutputStream()
    val compressed: OutputStream = codec match
      case 1 => GZIPOutputStream(output)
      case 2 => SnappyOutputStream(output)
      case 3 => LZ4FrameOutputStream(output)
      case 4 => ZstdOutputStreamNoFinalizer(output)
      case _ => throw CorruptRecordException(s"unsupported record compression type: $codec")
    try compressed.write(value)
    finally compressed.close()
    output.toByteArray

  private def readNullableBytes(buffer: ByteBuffer): Option[Vector[Byte]] =
    val length = readVarInt(buffer)
    if length == -1 then None
    else if length < -1 || length > buffer.remaining() then throw CorruptRecordException(s"invalid byte-array length: $length")
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
      if !buffer.hasRemaining then throw CorruptRecordException("truncated variable-length integer")
      val value = buffer.get().toInt & 0xff
      val payload = value & 0x7f
      if count == maximumBytes - 1 && payload > (if maximumBytes == 5 then 0x0f else 0x01) then
        throw CorruptRecordException("variable-length integer overflows its target type")
      result |= payload.toLong << shift
      complete = (value & 0x80) == 0
      shift += 7
      count += 1
    if !complete then throw CorruptRecordException("variable-length integer is too long")
    result

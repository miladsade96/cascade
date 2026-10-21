package cascade.storage

import cascade.protocol.ProtocolException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import java.util.zip.CRC32C

/** Atomic bounded snapshot of the producer batches needed for idempotent retry validation. */
private[storage] final class ProducerStateCheckpoint(path: Path, historyLimit: Int):
  require(historyLimit > 0, "producer history limit must be positive")

  private val Magic = 0x43505331 // CPS1
  private val FormatVersion: Short = 1
  private val HeaderBytes = 12
  private val EntryBytes = 52
  private val ChecksumBytes = 4
  private val MaximumEntries = 1_000_000
  private val MaximumBytes = 64 * 1024 * 1024
  private val temporary = path.resolveSibling(path.getFileName.toString + ".next")

  Option(path.getParent).foreach(directory => Files.createDirectories(directory): Unit)
  Files.deleteIfExists(temporary): Unit

  val recovered: Vector[RecordBatchMetadata] = load()

  def persist(entries: Iterable[RecordBatchMetadata]): Vector[RecordBatchMetadata] = synchronized {
    val retained = uniqueEntries(entries)
      .groupBy(_.producerId)
      .toVector
      .sortBy(_._1)
      .flatMap { case (_, batches) => batches.sortBy(_.baseOffset).takeRight(historyLimit) }
    if retained.size > MaximumEntries then
      throw ProtocolException(s"producer-state checkpoint has too many entries: ${retained.size}")
    val totalBytes =
      try Math.addExact(Math.addExact(HeaderBytes, Math.multiplyExact(retained.size, EntryBytes)), ChecksumBytes)
      catch case _: ArithmeticException => throw ProtocolException("producer-state checkpoint size overflow")
    if totalBytes > MaximumBytes then throw ProtocolException(s"producer-state checkpoint exceeds $MaximumBytes bytes")

    val buffer = ByteBuffer.allocate(totalBytes)
    buffer.putInt(Magic)
    buffer.putShort(FormatVersion)
    buffer.putShort(0.toShort)
    buffer.putInt(retained.size)
    retained.foreach(value => encodeEntry(buffer, value))
    val checksum = CRC32C()
    checksum.update(buffer.array(), 0, totalBytes - ChecksumBytes)
    buffer.putInt(checksum.getValue.toInt)
    buffer.flip()

    Files.deleteIfExists(temporary): Unit
    val channel = FileChannel.open(
      temporary,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.WRITE
    )
    try
      while buffer.hasRemaining do
        val written = channel.write(buffer)
        if written <= 0 then throw ProtocolException("producer-state checkpoint made no write progress")
      channel.force(true)
    finally channel.close()
    AtomicFileLifecycle.replace(temporary, path)
    retained
  }

  private def load(): Vector[RecordBatchMetadata] =
    if !Files.exists(path) then Vector.empty
    else
      val size = Files.size(path)
      if size < HeaderBytes + ChecksumBytes || size > MaximumBytes then
        throw ProtocolException(s"invalid producer-state checkpoint size: $size")
      val bytes = Files.readAllBytes(path)
      val buffer = ByteBuffer.wrap(bytes)
      val magic = buffer.getInt()
      val version = buffer.getShort()
      val reserved = buffer.getShort()
      val count = buffer.getInt()
      if magic != Magic || version != FormatVersion || reserved != 0 || count < 0 || count > MaximumEntries then
        throw ProtocolException("invalid producer-state checkpoint header")
      val expectedBytes =
        try Math.addExact(Math.addExact(HeaderBytes, Math.multiplyExact(count, EntryBytes)), ChecksumBytes)
        catch case _: ArithmeticException => throw ProtocolException("producer-state checkpoint size overflow")
      if expectedBytes != bytes.length then throw ProtocolException("producer-state checkpoint length mismatch")
      val expectedChecksum = ByteBuffer.wrap(bytes).getInt(bytes.length - ChecksumBytes)
      val checksum = CRC32C()
      checksum.update(bytes, 0, bytes.length - ChecksumBytes)
      if expectedChecksum != checksum.getValue.toInt then throw ProtocolException("producer-state checkpoint CRC32C mismatch")
      val entries = uniqueEntries(Vector.fill(count)(decodeEntry(buffer)))
        .sortBy(value => (value.producerId, value.baseOffset))
      if entries.groupBy(_.producerId).exists { case (_, values) => values.size > historyLimit } then
        throw ProtocolException("producer-state checkpoint contains unbounded history")
      entries

  private def uniqueEntries(entries: Iterable[RecordBatchMetadata]): Vector[RecordBatchMetadata] =
    val persisted = entries.iterator.filter(_.producerId >= 0L).toVector
    persisted.foreach(validateEntry)
    persisted.groupBy(value => value.producerId -> value.baseOffset).toVector.map { case ((producerId, baseOffset), values) =>
      val first = values.head
      if values.exists(_ != first) then
        throw ProtocolException(s"producer-state checkpoint conflicts for producer $producerId at offset $baseOffset")
      first
    }

  private def encodeEntry(buffer: ByteBuffer, value: RecordBatchMetadata): Unit =
    validateEntry(value)
    buffer.putLong(value.baseOffset)
    buffer.putLong(value.lastOffset)
    buffer.putLong(value.producerId)
    buffer.putShort(value.producerEpoch)
    val flags = (if value.transactional then 1 else 0) | (if value.control then 2 else 0)
    buffer.putShort(flags.toShort)
    buffer.putInt(value.baseSequence)
    buffer.putInt(value.lastSequence)
    buffer.putInt(value.recordCount)
    buffer.putLong(value.maxTimestamp)
    buffer.putInt(value.compressionType): Unit

  private def decodeEntry(buffer: ByteBuffer): RecordBatchMetadata =
    val baseOffset = buffer.getLong()
    val lastOffset = buffer.getLong()
    val producerId = buffer.getLong()
    val producerEpoch = buffer.getShort()
    val flags = buffer.getShort().toInt & 0xffff
    val baseSequence = buffer.getInt()
    val lastSequence = buffer.getInt()
    val recordCount = buffer.getInt()
    val maxTimestamp = buffer.getLong()
    val compressionType = buffer.getInt()
    if (flags & ~0x03) != 0 then throw ProtocolException("producer-state checkpoint has unknown flags")
    val value = RecordBatchMetadata(
      baseOffset,
      lastOffset,
      producerId,
      producerEpoch,
      baseSequence,
      lastSequence,
      recordCount,
      transactional = (flags & 1) != 0,
      control = (flags & 2) != 0,
      maxTimestamp,
      compressionType
    )
    validateEntry(value)
    value

  private def validateEntry(value: RecordBatchMetadata): Unit =
    if value.baseOffset < 0L || value.lastOffset < value.baseOffset || value.producerId < 0L ||
        value.producerEpoch < 0 || value.baseSequence < 0 || value.lastSequence < 0 || value.recordCount <= 0 ||
        value.compressionType < 0 || value.compressionType > 4
    then throw ProtocolException("invalid producer-state checkpoint entry")

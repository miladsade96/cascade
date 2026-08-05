package cascade.storage

import cascade.protocol.ProtocolException
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.*

final case class AppendResult(baseOffset: Long, lastOffset: Long)
final case class FetchResult(highWatermark: Long, logStartOffset: Long, records: Array[Byte])

private final case class BatchIndex(baseOffset: Long, lastOffset: Long, position: Long, size: Int)

private final class LogSegment(val baseOffset: Long, val path: Path):
  val channel: FileChannel = FileChannel.open(
    path,
    StandardOpenOption.CREATE,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE
  )
  val index: ArrayBuffer[BatchIndex] = ArrayBuffer.empty

  def size: Long = channel.size()

  def close(): Unit = channel.close()

/** Single-writer append log with immutable on-disk Kafka record batches and segment rollover. */
final class PartitionLog(directory: Path, maxSegmentBytes: Long = 128L * 1024 * 1024) extends AutoCloseable:
  require(maxSegmentBytes >= 1024, "maxSegmentBytes must be at least 1 KiB")
  Files.createDirectories(directory)

  private val segments = ArrayBuffer.empty[LogSegment]
  loadSegments()

  private var nextOffset: Long = segments.lastOption
    .flatMap(_.index.lastOption)
    .map(entry => Math.addExact(entry.lastOffset, 1L))
    .getOrElse(0L)

  def highWatermark: Long = synchronized(nextOffset)

  def logStartOffset: Long = synchronized {
    segments.iterator.flatMap(_.index.headOption).map(_.baseOffset).nextOption().getOrElse(nextOffset)
  }

  def append(recordSet: Array[Byte], force: Boolean): AppendResult = synchronized {
    val prepared = RecordBatch.prepare(recordSet, nextOffset)
    if prepared.isEmpty then throw ProtocolException("produce request contained an empty record set")
    val firstOffset = prepared.head.baseOffset
    prepared.foreach { batch =>
      val segment = writableSegment(batch.bytes.length, batch.baseOffset)
      val position = segment.channel.size()
      writeFully(segment.channel, ByteBuffer.wrap(batch.bytes), position)
      segment.index += BatchIndex(batch.baseOffset, batch.lastOffset, position, batch.bytes.length)
      nextOffset = Math.addExact(batch.lastOffset, 1L)
    }
    if force then segments.last.channel.force(false)
    AppendResult(firstOffset, nextOffset - 1)
  }

  def fetch(offset: Long, maxBytes: Int): FetchResult = synchronized {
    val output = ByteArrayOutputStream(math.min(math.max(maxBytes, 0), 1024 * 1024))
    var included = false
    segments.foreach { segment =>
      segment.index.foreach { entry =>
        if entry.lastOffset >= offset && (!included || output.size() + entry.size <= maxBytes) then
          val batch = new Array[Byte](entry.size)
          readFully(segment.channel, ByteBuffer.wrap(batch), entry.position)
          output.write(batch)
          included = true
      }
    }
    FetchResult(nextOffset, logStartOffset, output.toByteArray)
  }

  def offsetForTimestamp(timestamp: Long): Long = synchronized {
    if timestamp == -2L then logStartOffset
    else if timestamp == -1L then nextOffset
    else logStartOffset // Timestamp index is a later compatibility milestone.
  }

  override def close(): Unit = synchronized(segments.foreach(_.close()))

  private def writableSegment(batchSize: Int, batchBaseOffset: Long): LogSegment =
    segments.lastOption match
      case Some(segment) if segment.size == 0 || segment.size + batchSize <= maxSegmentBytes => segment
      case _ =>
        val segment = openSegment(batchBaseOffset)
        segments += segment
        segment

  private def loadSegments(): Unit =
    val paths = Files.list(directory)
    try
      paths.iterator().asScala
        .filter(path => path.getFileName.toString.matches("[0-9]{20}\\.log"))
        .toVector
        .sortBy(_.getFileName.toString)
        .foreach { path =>
          val base = path.getFileName.toString.stripSuffix(".log").toLong
          val segment = LogSegment(base, path)
          scan(segment)
          segments += segment
        }
    finally paths.close()
    if segments.isEmpty then segments += openSegment(0L)

  private def openSegment(baseOffset: Long): LogSegment =
    LogSegment(baseOffset, directory.resolve(f"$baseOffset%020d.log"))

  private def scan(segment: LogSegment): Unit =
    var position = 0L
    val fileSize = segment.channel.size()
    while position < fileSize do
      if fileSize - position < 12 then throw ProtocolException(s"truncated record batch in ${segment.path}")
      val prefix = new Array[Byte](12)
      readFully(segment.channel, ByteBuffer.wrap(prefix), position)
      val totalSize = RecordBatch.totalSize(prefix)
      if totalSize < 61 || position + totalSize > fileSize then
        throw ProtocolException(s"corrupt record batch in ${segment.path} at $position")
      val batch = new Array[Byte](totalSize)
      readFully(segment.channel, ByteBuffer.wrap(batch), position)
      segment.index += BatchIndex(RecordBatch.baseOffset(batch), RecordBatch.lastOffset(batch), position, totalSize)
      position += totalSize

  private def writeFully(channel: FileChannel, buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val written = channel.write(buffer, position)
      if written <= 0 then throw ProtocolException("file channel made no append progress")
      position += written

  private def readFully(channel: FileChannel, buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val read = channel.read(buffer, position)
      if read < 0 then throw ProtocolException("unexpected end of log segment")
      if read == 0 then throw ProtocolException("file channel made no read progress")
      position += read


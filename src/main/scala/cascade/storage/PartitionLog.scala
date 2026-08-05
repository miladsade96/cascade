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
private final case class FlushTarget(segment: LogSegment, bytes: Long, dirtySinceNanos: Long)

private final class LogSegment(val baseOffset: Long, val path: Path):
  val channel: FileChannel = FileChannel.open(
    path,
    StandardOpenOption.CREATE,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE
  )
  val index: ArrayBuffer[BatchIndex] = ArrayBuffer.empty
  var unflushedBytes: Long = 0L
  var dirtySinceNanos: Long = 0L

  def size: Long = channel.size()

  def close(): Unit = channel.close()

/** Single-writer append log with immutable on-disk Kafka record batches and segment rollover. */
final case class FlushStatistics(forces: Long, bytes: Long, nanos: Long, pendingBytes: Long):
  def +(other: FlushStatistics): FlushStatistics =
    FlushStatistics(forces + other.forces, bytes + other.bytes, nanos + other.nanos, pendingBytes + other.pendingBytes)

object FlushStatistics:
  val Empty: FlushStatistics = FlushStatistics(0L, 0L, 0L, 0L)

final class PartitionLog(
    directory: Path,
    maxSegmentBytes: Long = 128L * 1024 * 1024,
    flushPolicy: FlushPolicy = FlushPolicy.Periodic,
    flushIntervalMillis: Long = 1000L,
    maxUnflushedBytes: Long = 64L * 1024 * 1024,
    requestFlush: () => Unit = () => ()
) extends AutoCloseable:
  require(maxSegmentBytes >= 1024, "maxSegmentBytes must be at least 1 KiB")
  require(flushIntervalMillis > 0, "flush interval must be positive")
  require(maxUnflushedBytes > 0, "maximum unflushed bytes must be positive")
  Files.createDirectories(directory)

  private val flushIntervalNanos = Math.multiplyExact(flushIntervalMillis, 1_000_000L)

  private val segments = ArrayBuffer.empty[LogSegment]
  loadSegments()

  private var nextOffset: Long = segments.lastOption
    .flatMap(_.index.lastOption)
    .map(entry => Math.addExact(entry.lastOffset, 1L))
    .getOrElse(0L)
  private var unflushedBytes = 0L
  private var inFlightFlushBytes = 0L
  private var flushInProgress = false
  private var rolloverFlushRequested = false
  private var forceCount = 0L
  private var forcedBytes = 0L
  private var forceNanos = 0L

  def highWatermark: Long = synchronized(nextOffset)

  def logStartOffset: Long = synchronized {
    segments.iterator.flatMap(_.index.headOption).map(_.baseOffset).nextOption().getOrElse(nextOffset)
  }

  def append(recordSet: Array[Byte]): AppendResult =
    var scheduleFlush = false
    val result = synchronized {
      val prepared = RecordBatch.prepare(recordSet, nextOffset)
      if prepared.isEmpty then throw ProtocolException("produce request contained an empty record set")
      val firstOffset = prepared.head.baseOffset
      prepared.foreach { batch =>
        val segment = writableSegment(batch.bytes.length, batch.baseOffset)
        val position = segment.channel.size()
        writeFully(segment.channel, ByteBuffer.wrap(batch.bytes), position)
        segment.index += BatchIndex(batch.baseOffset, batch.lastOffset, position, batch.bytes.length)
        markDirty(segment, batch.bytes.length)
        nextOffset = Math.addExact(batch.lastOffset, 1L)
      }
      flushPolicy match
        case FlushPolicy.Sync => flushDirtySegments()
        case FlushPolicy.Periodic =>
          scheduleFlush = rolloverFlushRequested || unflushedBytes >= maxUnflushedBytes
      AppendResult(firstOffset, nextOffset - 1)
    }
    if scheduleFlush then requestFlush()
    result

  private[storage] def flushIfNeeded(nowNanos: Long = System.nanoTime()): Boolean = synchronized {
    val ageExceeded = segments.exists { segment =>
      segment.dirtySinceNanos != 0L && nowNanos - segment.dirtySinceNanos >= flushIntervalNanos
    }
    if flushPolicy == FlushPolicy.Periodic && !flushInProgress && unflushedBytes > 0L &&
        (rolloverFlushRequested || unflushedBytes >= maxUnflushedBytes || ageExceeded)
    then beginBackgroundFlush()
    else Vector.empty
  } match
    case targets if targets.nonEmpty =>
      forceInBackground(targets)
      true
    case _ => false

  private[storage] def flushStatistics: FlushStatistics = synchronized {
    FlushStatistics(forceCount, forcedBytes, forceNanos, unflushedBytes + inFlightFlushBytes)
  }

  def fetch(offset: Long, maxBytes: Int): FetchResult = synchronized {
    val output = ByteArrayOutputStream(math.min(math.max(maxBytes, 0), 1024 * 1024))
    var included = false
    var hasCapacity = true
    var segmentIndex = 0
    while segmentIndex < segments.length && hasCapacity do
      val segment = segments(segmentIndex)
      var entryIndex = 0
      while entryIndex < segment.index.length && hasCapacity do
        val entry = segment.index(entryIndex)
        if entry.lastOffset >= offset then
          val fits = output.size().toLong + entry.size.toLong <= maxBytes.toLong
          if included && !fits then hasCapacity = false
          else
            val batch = new Array[Byte](entry.size)
            readFully(segment.channel, ByteBuffer.wrap(batch), entry.position)
            output.write(batch)
            included = true
        entryIndex += 1
      segmentIndex += 1
    FetchResult(nextOffset, logStartOffset, output.toByteArray)
  }

  def offsetForTimestamp(timestamp: Long): Long = synchronized {
    if timestamp == -2L then logStartOffset
    else if timestamp == -1L then nextOffset
    else logStartOffset // Timestamp index is a later compatibility milestone.
  }

  override def close(): Unit = synchronized {
    awaitBackgroundFlush()
    flushDirtySegments()
    segments.foreach(_.close())
  }

  private def writableSegment(batchSize: Int, batchBaseOffset: Long): LogSegment =
    segments.lastOption match
      case Some(segment) if segment.size == 0 || segment.size + batchSize <= maxSegmentBytes => segment
      case _ =>
        rolloverFlushRequested = true
        val segment = openSegment(batchBaseOffset)
        segments += segment
        segment

  private def loadSegments(): Unit =
    val paths = Files.list(directory)
    try
      val segmentPaths = paths.iterator().asScala
        .filter(path => path.getFileName.toString.matches("[0-9]{20}\\.log"))
        .toVector
        .sortBy(_.getFileName.toString)
      var index = 0
      var recoveredTail = false
      while index < segmentPaths.length && !recoveredTail do
        val path = segmentPaths(index)
        val base = path.getFileName.toString.stripSuffix(".log").toLong
        val segment = LogSegment(base, path)
        recoveredTail = scan(segment)
        segments += segment
        if recoveredTail then segmentPaths.drop(index + 1).foreach(Files.delete)
        index += 1
    finally paths.close()
    if segments.isEmpty then segments += openSegment(0L)

  private def openSegment(baseOffset: Long): LogSegment =
    LogSegment(baseOffset, directory.resolve(f"$baseOffset%020d.log"))

  private def scan(segment: LogSegment): Boolean =
    var position = 0L
    val fileSize = segment.channel.size()
    var scanning = true
    while position < fileSize && scanning do
      if fileSize - position < 12 then
        scanning = false
      else
        val prefix = new Array[Byte](12)
        readFully(segment.channel, ByteBuffer.wrap(prefix), position)
        val totalSize = RecordBatch.totalSize(prefix)
        if totalSize < 61 then throw ProtocolException(s"corrupt record batch in ${segment.path} at $position")
        else if position + totalSize > fileSize then
          scanning = false
        else
          val batch = new Array[Byte](totalSize)
          readFully(segment.channel, ByteBuffer.wrap(batch), position)
          segment.index += BatchIndex(RecordBatch.baseOffset(batch), RecordBatch.lastOffset(batch), position, totalSize)
          position += totalSize
    if position < fileSize then
      segment.channel.truncate(position)
      segment.channel.force(true)
      true
    else false

  private def markDirty(segment: LogSegment, bytes: Int): Unit =
    if segment.unflushedBytes == 0L then segment.dirtySinceNanos = System.nanoTime()
    segment.unflushedBytes = Math.addExact(segment.unflushedBytes, bytes.toLong)
    unflushedBytes = Math.addExact(unflushedBytes, bytes.toLong)

  private def beginBackgroundFlush(): Vector[FlushTarget] =
    flushInProgress = true
    rolloverFlushRequested = false
    val targets = segments.iterator
      .filter(_.unflushedBytes > 0L)
      .map(segment => FlushTarget(segment, segment.unflushedBytes, segment.dirtySinceNanos))
      .toVector
    targets.foreach { target =>
      target.segment.unflushedBytes = 0L
      target.segment.dirtySinceNanos = 0L
      unflushedBytes -= target.bytes
      inFlightFlushBytes += target.bytes
    }
    targets

  private def forceInBackground(targets: Vector[FlushTarget]): Unit =
    var index = 0
    try
      while index < targets.length do
        val target = targets(index)
        val started = System.nanoTime()
        target.segment.channel.force(false)
        val elapsed = System.nanoTime() - started
        synchronized {
          inFlightFlushBytes -= target.bytes
          forceCount += 1L
          forcedBytes += target.bytes
          forceNanos += elapsed
        }
        index += 1
    catch
      case error: Throwable =>
        synchronized {
          targets.drop(index).foreach { target =>
            inFlightFlushBytes -= target.bytes
            val segment = target.segment
            if segment.unflushedBytes == 0L then segment.dirtySinceNanos = target.dirtySinceNanos
            else segment.dirtySinceNanos = math.min(segment.dirtySinceNanos, target.dirtySinceNanos)
            segment.unflushedBytes += target.bytes
            unflushedBytes += target.bytes
          }
        }
        throw error
    finally
      val reschedule = synchronized {
        flushInProgress = false
        notifyAll()
        rolloverFlushRequested || unflushedBytes >= maxUnflushedBytes
      }
      if reschedule then requestFlush()

  private def awaitBackgroundFlush(): Unit =
    while flushInProgress do wait()

  private def flushDirtySegments(): Unit =
    segments.foreach(flushSegment)

  private def flushSegment(segment: LogSegment): Unit =
    if segment.unflushedBytes > 0L then
      val started = System.nanoTime()
      segment.channel.force(false)
      val elapsed = System.nanoTime() - started
      val bytes = segment.unflushedBytes
      segment.unflushedBytes = 0L
      segment.dirtySinceNanos = 0L
      unflushedBytes -= bytes
      forceCount += 1L
      forcedBytes += bytes
      forceNanos += elapsed

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

package cascade.storage

import cascade.protocol.ProtocolException
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import scala.collection.mutable.{ArrayBuffer, ArrayDeque, HashMap}
import scala.jdk.CollectionConverters.*

final case class AppendResult(baseOffset: Long, lastOffset: Long)
final case class FetchResult(highWatermark: Long, lastStableOffset: Long, logStartOffset: Long, records: Array[Byte])
final case class BatchFingerprint(
    baseOffset: Long,
    lastOffset: Long,
    size: Int,
    digestHigh: Long,
    digestLow: Long
)

private final class BatchIndex(
    val metadata: RecordBatchMetadata,
    val position: Long,
    val size: Int
):
  var prefixDigest: Option[(Long, Long)] = None
  def baseOffset: Long = metadata.baseOffset
  def lastOffset: Long = metadata.lastOffset
private final case class FlushTarget(segment: LogSegment, bytes: Long, dirtySinceNanos: Long)

private final class LogSegment(val baseOffset: Long, val path: Path):
  val channel: FileChannel = FileChannel.open(
    path,
    StandardOpenOption.CREATE,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE
  )
  val index: ArrayBuffer[BatchIndex] = ArrayBuffer.empty
  val timestampIndex: TimestampIndex = TimestampIndex()
  val transactionIndex: TransactionIndex = TransactionIndex()
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
  private val producerHistoryLimit = 5
  private val highWatermarkCheckpoint = HighWatermarkCheckpoint(directory.resolve("high-watermark.checkpoint"))

  private val segments = ArrayBuffer.empty[LogSegment]
  private val recentProducerBatches = HashMap.empty[Long, ArrayDeque[RecordBatchMetadata]]
  loadSegments()

  private var nextOffset: Long = segments.lastOption
    .flatMap(_.index.lastOption)
    .map(entry => Math.addExact(entry.lastOffset, 1L))
    .getOrElse(0L)
  private var committedOffset: Long = highWatermarkCheckpoint.offset match
    case Some(offset) => math.max(logStartOffset, math.min(offset, nextOffset))
    case None if highWatermarkCheckpoint.existed => logStartOffset
    case None => nextOffset // Upgrade compatibility for logs created before checkpoints existed.
  if highWatermarkCheckpoint.offset.forall(_ != committedOffset) then highWatermarkCheckpoint.persist(committedOffset)
  private var durableOffset: Long = nextOffset
  private var unflushedBytes = 0L
  private var inFlightFlushBytes = 0L
  private var flushInProgress = false
  private var rolloverFlushRequested = false
  private var forceCount = 0L
  private var forcedBytes = 0L
  private var forceNanos = 0L

  def highWatermark: Long = synchronized(committedOffset)

  def logEndOffset: Long = synchronized(nextOffset)

  def logStartOffset: Long = synchronized {
    segments.iterator.flatMap(_.index.headOption).map(_.baseOffset).nextOption().getOrElse(nextOffset)
  }

  def append(recordSet: Array[Byte]): AppendResult = appendInternal(recordSet, commitImmediately = true)

  def appendReplica(recordSet: Array[Byte], expectedBaseOffset: Long): AppendResult = synchronized {
    if nextOffset != expectedBaseOffset then
      throw ProtocolException(s"replica offset mismatch: expected $expectedBaseOffset, log-end=$nextOffset")
    appendInternal(recordSet, commitImmediately = false)
  }

  /** Returns bounded immutable batch identities used to find a replica's common prefix. */
  def recoverySummary(startOffset: Long, endOffsetExclusive: Long, maxEntries: Int): Vector[BatchFingerprint] =
    synchronized {
      if maxEntries <= 0 then throw ProtocolException("recovery summary entry limit must be positive")
      if startOffset < logStartOffset || endOffsetExclusive < startOffset || endOffsetExclusive > nextOffset then
        throw ProtocolException(
          s"invalid recovery summary range [$startOffset,$endOffsetExclusive); " +
            s"log-start=$logStartOffset, log-end=$nextOffset"
        )
      val entries = segments.iterator.flatMap(_.index.iterator).filter { entry =>
        entry.baseOffset >= startOffset && entry.lastOffset < endOffsetExclusive
      }.take(maxEntries)
      entries.map(entry => batchFingerprint(entry)).toVector
    }

  def recoveryProbe(offsetInclusive: Long, endOffsetExclusive: Long): Option[BatchFingerprint] = synchronized {
    if offsetInclusive < logStartOffset || endOffsetExclusive < logStartOffset || endOffsetExclusive > nextOffset then
      throw ProtocolException(
        s"invalid recovery probe offset=$offsetInclusive, end=$endOffsetExclusive; " +
          s"log-start=$logStartOffset, log-end=$nextOffset"
      )
    segments.iterator
      .flatMap(_.index.iterator)
      .filter(entry => entry.baseOffset <= offsetInclusive && entry.lastOffset < endOffsetExclusive)
      .reduceOption((_, right) => right)
      .map(batchFingerprint)
  }

  def recoveryFingerprint(baseOffset: Long): Option[BatchFingerprint] = synchronized {
    segments.iterator
      .flatMap(_.index.iterator)
      .find(_.baseOffset == baseOffset)
      .map(batchFingerprint)
  }

  /** Truncates only the divergent suffix while preserving the verified common prefix. */
  def truncateReplicaTo(offsetExclusive: Long): Unit = synchronized {
    val start = logStartOffset
    val boundary = offsetExclusive == start || offsetExclusive == nextOffset || segments.iterator
      .flatMap(_.index.iterator)
      .exists(entry => Math.addExact(entry.lastOffset, 1L) == offsetExclusive)
    if offsetExclusive < start || offsetExclusive > nextOffset || !boundary then
      throw ProtocolException(
        s"replica truncation offset $offsetExclusive is not a batch boundary in [$start,$nextOffset]"
      )
    if offsetExclusive == nextOffset then
      if committedOffset > offsetExclusive then
        committedOffset = offsetExclusive
        checkpointDurableWatermark()
    else if offsetExclusive == start then resetReplica(start)
    else
      awaitBackgroundFlush()
      flushDirtySegments()
      val cutIndex = segments.indexWhere(_.index.exists(_.baseOffset >= offsetExclusive))
      if cutIndex < 0 then throw ProtocolException(s"missing replica suffix at $offsetExclusive")
      val cutSegment = segments(cutIndex)
      val retained = cutSegment.index.takeWhile(_.lastOffset < offsetExclusive)
      val cutPosition = retained.lastOption.map(entry => entry.position + entry.size.toLong).getOrElse(0L)
      cutSegment.channel.truncate(cutPosition)
      cutSegment.channel.force(true)
      cutSegment.index.remove(retained.size, cutSegment.index.size - retained.size)
      rebuildAuxiliaryIndexes(cutSegment)
      segments.drop(cutIndex + 1).foreach { segment =>
        segment.close()
        Files.deleteIfExists(segment.path): Unit
      }
      segments.remove(cutIndex + 1, segments.size - cutIndex - 1)
      recentProducerBatches.clear()
      segments.iterator.flatMap(_.index.iterator).foreach(entry => indexProducerBatch(entry.metadata))
      nextOffset = offsetExclusive
      committedOffset = math.min(committedOffset, offsetExclusive)
      durableOffset = offsetExclusive
      unflushedBytes = 0L
      inFlightFlushBytes = 0L
      rolloverFlushRequested = false
      checkpointDurableWatermark()
  }

  /**
   * Drops a replica's local copy before an authoritative leader streams a new committed prefix.
   * This is only safe while the replica is outside the ISR and the partition is fenced by the
   * replication manager.
   */
  def resetReplica(startOffset: Long): Unit = synchronized {
    if startOffset < 0L then throw ProtocolException(s"negative replica start offset: $startOffset")
    awaitBackgroundFlush()
    segments.foreach(_.close())
    segments.foreach(segment => Files.deleteIfExists(segment.path): Unit)
    segments.clear()
    recentProducerBatches.clear()
    nextOffset = startOffset
    committedOffset = startOffset
    durableOffset = startOffset
    unflushedBytes = 0L
    inFlightFlushBytes = 0L
    flushInProgress = false
    rolloverFlushRequested = false
    segments += openSegment(startOffset)
    highWatermarkCheckpoint.persist(startOffset)
    ()
  }

  def commitThrough(offsetExclusive: Long): Unit = synchronized {
    if offsetExclusive < committedOffset || offsetExclusive > nextOffset then
      throw ProtocolException(
        s"invalid commit watermark $offsetExclusive; current=$committedOffset, log-end=$nextOffset"
      )
    if offsetExclusive != committedOffset then
      committedOffset = offsetExclusive
      checkpointDurableWatermark()
  }

  private def appendInternal(recordSet: Array[Byte], commitImmediately: Boolean): AppendResult =
    var scheduleFlush = false
    val result = synchronized {
      val prepared = RecordBatch.prepare(recordSet, nextOffset)
      if prepared.isEmpty then throw ProtocolException("produce request contained an empty record set")
      val firstOffset = prepared.head.baseOffset
      prepared.foreach { batch =>
        val segment = writableSegment(batch.bytes.length, batch.baseOffset)
        val position = segment.channel.size()
        writeFully(segment.channel, ByteBuffer.wrap(batch.bytes), position)
        val metadata = RecordBatch.metadata(batch.bytes)
        segment.index += BatchIndex(metadata, position, batch.bytes.length)
        segment.timestampIndex.append(metadata)
        segment.transactionIndex.append(metadata)
        indexProducerBatch(metadata)
        markDirty(segment, batch.bytes.length)
        nextOffset = Math.addExact(batch.lastOffset, 1L)
      }
      flushPolicy match
        case FlushPolicy.Sync => flushDirtySegments()
        case FlushPolicy.Periodic =>
          scheduleFlush = rolloverFlushRequested || unflushedBytes >= maxUnflushedBytes
      if commitImmediately && committedOffset != nextOffset then
        committedOffset = nextOffset
        checkpointDurableWatermark()
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
    then (beginBackgroundFlush(), nextOffset)
    else (Vector.empty, durableOffset)
  } match
    case (targets, durableThrough) if targets.nonEmpty =>
      forceInBackground(targets, durableThrough)
      true
    case _ => false

  private[storage] def flushStatistics: FlushStatistics = synchronized {
    FlushStatistics(forceCount, forcedBytes, forceNanos, unflushedBytes + inFlightFlushBytes)
  }

  def fetch(offset: Long, maxBytes: Int): FetchResult =
    fetch(offset, maxBytes, highWatermark, _ => true)

  def fetch(
      offset: Long,
      maxBytes: Int,
      lastStableOffset: Long,
      include: RecordBatchMetadata => Boolean
  ): FetchResult = synchronized {
    val visibleEnd = math.min(committedOffset, lastStableOffset)
    val output = ByteArrayOutputStream(math.min(math.max(maxBytes, 0), 1024 * 1024))
    var included = false
    var hasCapacity = true
    var segmentIndex = 0
    while segmentIndex < segments.length && hasCapacity do
      val segment = segments(segmentIndex)
      var entryIndex = 0
      while entryIndex < segment.index.length && hasCapacity do
        val entry = segment.index(entryIndex)
        if entry.lastOffset >= offset && entry.lastOffset < visibleEnd && include(entry.metadata) then
          val fits = output.size().toLong + entry.size.toLong <= maxBytes.toLong
          if included && !fits then hasCapacity = false
          else
            val batch = new Array[Byte](entry.size)
            readFully(segment.channel, ByteBuffer.wrap(batch), entry.position)
            output.write(batch)
            included = true
        entryIndex += 1
      segmentIndex += 1
    FetchResult(committedOffset, visibleEnd, logStartOffset, output.toByteArray)
  }

  def producerBatches(producerId: Long): Vector[RecordBatchMetadata] = synchronized {
    segments.iterator
      .flatMap(_.index.iterator)
      .map(_.metadata)
      .filter(_.producerId == producerId)
      .toVector
  }

  /** The bounded Kafka duplicate-detection window, kept off the append path's full segment index. */
  def recentBatches(producerId: Long): Vector[RecordBatchMetadata] = synchronized {
    recentProducerBatches.get(producerId).fold(Vector.empty)(_.toVector)
  }

  def offsetForTimestamp(timestamp: Long): Long = synchronized {
    if timestamp == -2L then logStartOffset
    else if timestamp == -1L then committedOffset
    else
      segments.iterator.flatMap(_.timestampIndex.offsetFor(timestamp)).nextOption().getOrElse(-1L)
  }

  def transactionBatches(firstOffset: Long, lastOffsetExclusive: Long): Vector[TransactionIndexEntry] = synchronized {
    segments.iterator.flatMap(_.transactionIndex.overlapping(firstOffset, lastOffsetExclusive)).toVector
  }

  override def close(): Unit = synchronized {
    awaitBackgroundFlush()
    flushDirtySegments()
    highWatermarkCheckpoint.persist(committedOffset)
    segments.foreach(_.close())
    highWatermarkCheckpoint.close()
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
          val metadata = RecordBatch.metadata(batch)
          segment.index += BatchIndex(metadata, position, totalSize)
          segment.timestampIndex.append(metadata)
          segment.transactionIndex.append(metadata)
          indexProducerBatch(metadata)
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

  private def batchFingerprint(entry: BatchIndex): BatchFingerprint =
    ensureFingerprint(entry)
    val (high, low) = entry.prefixDigest.getOrElse(throw ProtocolException("missing recovery fingerprint"))
    BatchFingerprint(entry.baseOffset, entry.lastOffset, entry.size, high, low)

  private def ensureFingerprint(target: BatchIndex): Unit =
    var previousHigh = 0L
    var previousLow = 0L
    val iterator = segments.iterator.flatMap { segment => segment.index.iterator.map(segment -> _) }
    var found = false
    while iterator.hasNext && !found do
      val (segment, entry) = iterator.next()
      entry.prefixDigest match
        case Some((high, low)) =>
          previousHigh = high
          previousLow = low
        case None =>
          val bytes = new Array[Byte](entry.size)
          readFully(segment.channel, ByteBuffer.wrap(bytes), entry.position)
          val digest = MessageDigest.getInstance("SHA-256")
          digest.update(ByteBuffer.allocate(16).putLong(previousHigh).putLong(previousLow).array())
          val values = ByteBuffer.wrap(digest.digest(bytes))
          previousHigh = values.getLong()
          previousLow = values.getLong()
          entry.prefixDigest = Some((previousHigh, previousLow))
      found = entry eq target
    if !found then throw ProtocolException(s"missing batch ${target.baseOffset} for recovery fingerprint")

  private def indexProducerBatch(metadata: RecordBatchMetadata): Unit =
    if metadata.producerId >= 0L then
      val history = recentProducerBatches.getOrElseUpdate(metadata.producerId, ArrayDeque.empty)
      history.append(metadata)
      while history.length > producerHistoryLimit do history.removeHead(): Unit

  private def rebuildAuxiliaryIndexes(segment: LogSegment): Unit =
    segment.timestampIndex.clear()
    segment.transactionIndex.clear()
    segment.index.foreach { entry =>
      segment.timestampIndex.append(entry.metadata)
      segment.transactionIndex.append(entry.metadata)
    }

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

  private def forceInBackground(targets: Vector[FlushTarget], durableThrough: Long): Unit =
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
      synchronized {
        durableOffset = math.max(durableOffset, durableThrough)
        checkpointDurableWatermark()
      }
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
    durableOffset = nextOffset
    checkpointDurableWatermark()

  private def checkpointDurableWatermark(): Unit =
    highWatermarkCheckpoint.persist(math.min(committedOffset, durableOffset))

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

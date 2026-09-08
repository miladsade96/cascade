package cascade.coordinator

import cascade.protocol.ProtocolException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.zip.CRC32C
import scala.collection.mutable.ArrayBuffer

final case class CoordinatorShardJournalSnapshot(
    records: Long = 0L,
    bytes: Long = 0L,
    forceNanos: Long = 0L,
    truncatedBytes: Long = 0L
)

/** Forced, checksummed transaction log for one virtual coordinator shard. */
final class CoordinatorShardJournal(path: Path, val shard: Int) extends AutoCloseable:
  require(CoordinatorShard.valid(shard), "invalid coordinator shard ID")
  private val target = path.toAbsolutePath.normalize()
  Option(target.getParent).foreach(Files.createDirectories(_))
  private val channel = FileChannel.open(
    target,
    StandardOpenOption.CREATE,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE
  )
  private val recovered = ArrayBuffer.empty[CoordinatorQuorumRecord]
  private var statistics = CoordinatorShardJournalSnapshot()
  private var appendPosition = recover()

  def entries: Vector[CoordinatorQuorumRecord] = synchronized(recovered.toVector)

  def snapshot: CoordinatorShardJournalSnapshot = synchronized(statistics.copy(bytes = appendPosition))

  def append(record: CoordinatorQuorumRecord): Unit = synchronized {
    record.delta.foreach { delta =>
      if !delta.updates.exists(_.id == shard) then
        throw IllegalArgumentException(s"prepare does not update coordinator shard $shard")
    }
    val payload = CoordinatorQuorumRecordCodec.encode(record)
    val checksum = CRC32C()
    checksum.update(payload, 0, payload.length)
    val frame = ByteBuffer.allocate(payload.length + 8)
      .putInt(payload.length)
      .put(payload)
      .putInt(checksum.getValue.toInt)
      .flip()
    channel.position(appendPosition)
    writeFully(frame)
    val started = System.nanoTime()
    channel.force(true)
    val forced = math.max(0L, System.nanoTime() - started)
    appendPosition = Math.addExact(appendPosition, payload.length.toLong + 8L)
    recovered += record
    statistics = statistics.copy(records = statistics.records + 1L, forceNanos = statistics.forceNanos + forced)
  }

  override def close(): Unit = synchronized(channel.close())

  private def recover(): Long =
    val size = channel.size()
    var position = 0L
    while size - position >= 4L do
      channel.position(position)
      val header = ByteBuffer.allocate(4)
      readFully(header) match
        case false => return truncateTail(position, size)
        case true => ()
      val length = header.flip().getInt()
      if length <= 0 || length > CoordinatorQuorumRecordCodec.MaximumBytes then
        return truncateTail(position, size)
      val frameBytes = length.toLong + 8L
      if size - position < frameBytes then return truncateTail(position, size)
      val payload = ByteBuffer.allocate(length)
      if !readFully(payload) then return truncateTail(position, size)
      val checksumBuffer = ByteBuffer.allocate(4)
      if !readFully(checksumBuffer) then return truncateTail(position, size)
      val bytes = payload.array()
      val checksum = CRC32C()
      checksum.update(bytes, 0, bytes.length)
      if checksum.getValue.toInt != checksumBuffer.flip().getInt() then return truncateTail(position, size)
      val record = CoordinatorQuorumRecordCodec.decode(bytes)
      record.delta.foreach { delta =>
        if !delta.updates.exists(_.id == shard) then
          throw ProtocolException(s"coordinator shard $shard journal contains a foreign prepare")
      }
      recovered += record
      position += frameBytes
    if position != size then truncateTail(position, size) else position

  private def truncateTail(position: Long, size: Long): Long =
    channel.truncate(position)
    channel.force(true)
    statistics = statistics.copy(truncatedBytes = statistics.truncatedBytes + math.max(0L, size - position))
    position

  private def writeFully(buffer: ByteBuffer): Unit =
    while buffer.hasRemaining do
      if channel.write(buffer) <= 0 then throw ProtocolException("coordinator shard journal made no append progress")

  private def readFully(buffer: ByteBuffer): Boolean =
    while buffer.hasRemaining do
      val count = channel.read(buffer)
      if count < 0 then return false
      if count == 0 then throw ProtocolException("coordinator shard journal made no read progress")
    true

object CoordinatorShardJournal:
  def path(directory: Path, shard: Int): Path =
    require(CoordinatorShard.valid(shard), "invalid coordinator shard ID")
    directory.resolve(f"shard-$shard%03d.log")

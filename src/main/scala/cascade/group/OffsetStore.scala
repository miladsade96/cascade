package cascade.group

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.storage.AtomicFileLifecycle
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import java.util.zip.CRC32C
import scala.collection.mutable

final case class GroupOffsetKey(groupId: String, topic: String, partition: Int)
final case class CommittedOffset(offset: Long, leaderEpoch: Int, metadata: Option[String], committedAtMillis: Long)
final case class OffsetCommitValue(key: GroupOffsetKey, value: CommittedOffset)

/** Durable append-only committed-offset journal with checksum validation and partial-tail recovery. */
final class OffsetStore(path: Path) extends AutoCloseable:
  private val RecordVersion: Short = 1
  private val MaximumRecordBytes = 1024 * 1024

  Option(path.getParent).foreach(directory => Files.createDirectories(directory): Unit)
  Option(path.getParent).foreach(AtomicFileLifecycle.recoverReplacements)
  private var channel = openChannel()
  private val offsets = mutable.HashMap.empty[GroupOffsetKey, CommittedOffset]
  private var appendPosition = recover()
  private var closed = false

  def commit(values: Vector[OffsetCommitValue], durable: Boolean = true): Unit = synchronized {
    ensureOpen()
    if values.nonEmpty then
      if durable then
        val frames = values.map(encode)
        frames.foreach { frame =>
          writeFully(ByteBuffer.wrap(frame), appendPosition)
          appendPosition += frame.length
        }
        channel.force(false)
      values.foreach(value => offsets.update(value.key, value.value))
  }

  def entries: Vector[OffsetCommitValue] = synchronized {
    offsets.iterator
      .map { case (key, value) => OffsetCommitValue(key, value) }
      .toVector
      .sortBy(value => (value.key.groupId, value.key.topic, value.key.partition))
  }

  /** Replaces the in-memory view after installing an authoritative quorum snapshot. */
  def install(values: Vector[OffsetCommitValue]): Unit = synchronized {
    ensureOpen()
    offsets.clear()
    values.foreach(value => offsets.update(value.key, value.value))
  }

  def get(key: GroupOffsetKey): Option[CommittedOffset] = synchronized(offsets.get(key))

  def all(groupId: String): Vector[(GroupOffsetKey, CommittedOffset)] = synchronized {
    offsets.iterator.filter(_._1.groupId == groupId).toVector.sortBy { case (key, _) => (key.topic, key.partition) }
  }

  def journalSize: Long = synchronized(channel.size())

  /** Rewrites only the latest value for every key and installs it with one atomic rename. */
  def compact(): Unit = synchronized {
    ensureOpen()
    val temporary = path.resolveSibling(path.getFileName.toString + ".cleaned")
    Files.deleteIfExists(temporary): Unit
    val output = FileChannel.open(
      temporary,
      StandardOpenOption.CREATE_NEW,
      StandardOpenOption.READ,
      StandardOpenOption.WRITE
    )
    var position = 0L
    try
      entries.foreach { entry =>
        val frame = encode(entry)
        writeFully(output, ByteBuffer.wrap(frame), position)
        position += frame.length
      }
      output.force(true)
    finally output.close()
    channel.force(false)
    channel.close()
    AtomicFileLifecycle.replace(temporary, path)
    channel = openChannel()
    appendPosition = position
  }

  override def close(): Unit = synchronized {
    if !closed then
      channel.force(false)
      channel.close()
      closed = true
  }

  private def encode(entry: OffsetCommitValue): Array[Byte] =
    val payload = ByteWriter()
      .writeShort(RecordVersion)
      .writeString(entry.key.groupId)
      .writeString(entry.key.topic)
      .writeInt(entry.key.partition)
      .writeLong(entry.value.offset)
      .writeInt(entry.value.leaderEpoch)
      .writeNullableString(entry.value.metadata)
      .writeLong(entry.value.committedAtMillis)
      .result()
    val checksum = CRC32C()
    checksum.update(payload, 0, payload.length)
    ByteWriter(payload.length + 8)
      .writeInt(payload.length)
      .writeBytes(payload)
      .writeInt(checksum.getValue.toInt)
      .result()

  private def recover(): Long =
    val fileSize = channel.size()
    var position = 0L
    var scanning = true
    while position < fileSize && scanning do
      if fileSize - position < 4L then scanning = false
      else
        val lengthBytes = new Array[Byte](4)
        readFully(ByteBuffer.wrap(lengthBytes), position)
        val length = ByteCursor(lengthBytes).readInt()
        if length <= 0 || length > MaximumRecordBytes then
          throw ProtocolException(s"invalid committed-offset record length $length at $position")
        val frameSize = 4L + length + 4L
        if position + frameSize > fileSize then scanning = false
        else
          val payload = new Array[Byte](length)
          readFully(ByteBuffer.wrap(payload), position + 4L)
          val checksumBytes = new Array[Byte](4)
          readFully(ByteBuffer.wrap(checksumBytes), position + 4L + length)
          val expectedChecksum = ByteCursor(checksumBytes).readInt()
          val checksum = CRC32C()
          checksum.update(payload, 0, payload.length)
          if checksum.getValue.toInt != expectedChecksum then scanning = false
          else
            val entry = decode(payload)
            offsets.update(entry.key, entry.value)
            position += frameSize
    if position < fileSize then
      channel.truncate(position)
      channel.force(true)
    position

  private def decode(payload: Array[Byte]): OffsetCommitValue =
    val cursor = ByteCursor(payload)
    val version = cursor.readShort()
    if version != RecordVersion then throw ProtocolException(s"unsupported committed-offset record version: $version")
    val key = GroupOffsetKey(cursor.readString(), cursor.readString(), cursor.readInt())
    val value = CommittedOffset(cursor.readLong(), cursor.readInt(), cursor.readNullableString(), cursor.readLong())
    cursor.ensureFullyRead()
    OffsetCommitValue(key, value)

  private def ensureOpen(): Unit =
    if closed then throw IllegalStateException("offset store is closed")

  private def writeFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val written = channel.write(buffer, position)
      if written <= 0 then throw ProtocolException("offset store made no append progress")
      position += written

  private def writeFully(target: FileChannel, buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val written = target.write(buffer, position)
      if written <= 0 then throw ProtocolException("offset compaction made no write progress")
      position += written

  private def readFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val read = channel.read(buffer, position)
      if read < 0 then throw ProtocolException("unexpected end of committed-offset journal")
      if read == 0 then throw ProtocolException("offset store made no read progress")
      position += read

  private def openChannel(): FileChannel =
    FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)

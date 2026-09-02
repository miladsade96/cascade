package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.storage.AtomicFileLifecycle
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import java.util.zip.CRC32C

/** Forced atomic journal of full checkpoints and exact-base coordinator deltas. */
final class MetadataStore(path: Path, compactionBytes: Long = Long.MaxValue) extends AutoCloseable:
  require(compactionBytes >= 1024L, "metadata compaction threshold must be at least 1 KiB")
  private val MaximumImageBytes = 64 * 1024 * 1024
  Option(path.getParent).foreach(directory => Files.createDirectories(directory): Unit)
  Option(path.getParent).foreach(AtomicFileLifecycle.recoverReplacements)
  private var channel = openChannel()
  private var closed = false
  private var appendPosition = 0L
  private var current =
    try recover()
    catch
      case error: Throwable =>
        channel.close()
        throw error

  def metadata: ClusterMetadata = synchronized(current)

  def commit(next: ClusterMetadata): Unit = synchronized {
    ensureOpen()
    if next.version > current.version then
      val payload = MetadataDelta.between(current, next).map(MetadataDeltaCodec.encode).getOrElse(MetadataCodec.encode(next))
      val frame = encodeFrame(payload)
      writeFully(ByteBuffer.wrap(frame), appendPosition)
      channel.force(false)
      appendPosition += frame.length
      current = next
      if appendPosition >= compactionBytes then compactCurrent()
  }

  def journalSize: Long = synchronized(channel.size())

  override def close(): Unit = synchronized {
    if !closed then
      closed = true
      AtomicFileLifecycle.forceAndClose(channel)
  }

  private def recover(): ClusterMetadata =
    val fileSize = channel.size()
    var position = 0L
    var latest = ClusterMetadata.Empty
    var scanning = true
    while position < fileSize && scanning do
      if fileSize - position < 4L then scanning = false
      else
        val lengthBytes = new Array[Byte](4)
        readFully(ByteBuffer.wrap(lengthBytes), position)
        val length = ByteCursor(lengthBytes).readInt()
        val frameSize = 4L + length.toLong + 4L
        if length <= 0 || length > MaximumImageBytes || position + frameSize > fileSize then scanning = false
        else
          val payload = new Array[Byte](length)
          readFully(ByteBuffer.wrap(payload), position + 4L)
          val checksumBytes = new Array[Byte](4)
          readFully(ByteBuffer.wrap(checksumBytes), position + 4L + length)
          val expected = ByteCursor(checksumBytes).readInt()
          val checksum = CRC32C()
          checksum.update(payload, 0, payload.length)
          if checksum.getValue.toInt != expected then scanning = false
          else
            val decoded =
              if MetadataDeltaCodec.isDelta(payload) then
                MetadataDeltaCodec.decode(payload).applyTo(latest)
                  .fold(message => throw ProtocolException(s"invalid metadata delta at $position: $message"), identity)
              else MetadataCodec.decode(payload)
            if decoded.version <= latest.version then
              throw ProtocolException(s"non-monotonic metadata version ${decoded.version} at $position")
            latest = decoded
            position += frameSize
    if position < fileSize then
      channel.truncate(position)
      channel.force(true)
    appendPosition = position
    latest

  private def ensureOpen(): Unit =
    if closed then throw IllegalStateException("metadata store is closed")

  private def writeFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val written = channel.write(buffer, position)
      if written <= 0 then throw ProtocolException("metadata journal made no append progress")
      position += written

  private def writeFully(target: FileChannel, buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val written = target.write(buffer, position)
      if written <= 0 then throw ProtocolException("metadata compaction made no write progress")
      position += written

  private def readFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val read = channel.read(buffer, position)
      if read < 0 then throw ProtocolException("unexpected end of metadata journal")
      if read == 0 then throw ProtocolException("metadata journal made no read progress")
      position += read

  private def compactCurrent(): Unit =
    val frame = encodeFrame(MetadataCodec.encode(current))
    val temporary = path.resolveSibling(path.getFileName.toString + ".cleaned")
    Files.deleteIfExists(temporary): Unit
    val output = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
    try
      writeFully(output, ByteBuffer.wrap(frame), 0L)
      output.force(true)
    finally output.close()
    channel.force(false)
    channel.close()
    AtomicFileLifecycle.replace(temporary, path)
    channel = openChannel()
    appendPosition = frame.length

  private def encodeFrame(payload: Array[Byte]): Array[Byte] =
    if payload.length <= 0 || payload.length > MaximumImageBytes then
      throw ProtocolException("metadata journal frame exceeds the supported size")
    val checksum = CRC32C()
    checksum.update(payload, 0, payload.length)
    ByteWriter(payload.length + 8)
      .writeInt(payload.length)
      .writeBytes(payload)
      .writeInt(checksum.getValue.toInt)
      .result()

  private def openChannel(): FileChannel =
    FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)

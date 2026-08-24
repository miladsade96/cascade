package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.storage.AtomicFileLifecycle
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import java.util.zip.CRC32C

final case class ControllerState(term: Long, votedFor: Option[Int]):
  require(term >= 0L, "controller term must be non-negative")
  require(votedFor.forall(_ >= 0), "controller vote must be a non-negative node ID")

object ControllerState:
  val Empty: ControllerState = ControllerState(0L, None)

/** Forced, checksum-protected election term and vote journal. */
final class ControllerStateStore(path: Path) extends AutoCloseable:
  private val FormatVersion: Short = 1
  Option(path.getParent).foreach(directory => Files.createDirectories(directory): Unit)
  private val channel = FileChannel.open(
    path,
    StandardOpenOption.CREATE,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE
  )
  private var closed = false
  private var appendPosition = 0L
  private var current = recover()

  def state: ControllerState = synchronized(current)

  def persist(next: ControllerState): Unit = synchronized {
    ensureOpen()
    if next.term < current.term then
      throw ProtocolException(s"controller term moved backwards: current=${current.term}, next=${next.term}")
    if next.term == current.term && current.votedFor.nonEmpty && next.votedFor != current.votedFor then
      throw ProtocolException(s"controller vote changed in term ${next.term}")
    if next != current then
      val payload = ByteWriter()
        .writeShort(FormatVersion)
        .writeLong(next.term)
        .writeInt(next.votedFor.getOrElse(-1))
        .result()
      val checksum = CRC32C()
      checksum.update(payload, 0, payload.length)
      val frame = ByteWriter(payload.length + 8)
        .writeInt(payload.length)
        .writeBytes(payload)
        .writeInt(checksum.getValue.toInt)
        .result()
      writeFully(ByteBuffer.wrap(frame), appendPosition)
      channel.force(false)
      appendPosition += frame.length
      current = next
  }

  override def close(): Unit = synchronized {
    if !closed then
      closed = true
      AtomicFileLifecycle.forceAndClose(channel)
  }

  private def recover(): ControllerState =
    val fileSize = channel.size()
    var position = 0L
    var latest = ControllerState.Empty
    var scanning = true
    while position < fileSize && scanning do
      if fileSize - position < 4L then scanning = false
      else
        val lengthBytes = new Array[Byte](4)
        readFully(ByteBuffer.wrap(lengthBytes), position)
        val length = ByteCursor(lengthBytes).readInt()
        val frameSize = 4L + length.toLong + 4L
        if length <= 0 || length > 1024 || position + frameSize > fileSize then scanning = false
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
            val decoded = decode(payload)
            if decoded.term < latest.term ||
                (decoded.term == latest.term && latest.votedFor.nonEmpty && decoded.votedFor != latest.votedFor)
            then throw ProtocolException(s"invalid controller election transition at $position")
            latest = decoded
            position += frameSize
    if position < fileSize then
      channel.truncate(position)
      channel.force(true)
    appendPosition = position
    latest

  private def decode(payload: Array[Byte]): ControllerState =
    val cursor = ByteCursor(payload)
    val format = cursor.readShort()
    if format != FormatVersion then throw ProtocolException(s"unsupported controller state format: $format")
    val term = cursor.readLong()
    val vote = cursor.readInt()
    cursor.ensureFullyRead()
    ControllerState(term, Option.when(vote >= 0)(vote))

  private def ensureOpen(): Unit =
    if closed then throw IllegalStateException("controller state store is closed")

  private def writeFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val written = channel.write(buffer, position)
      if written <= 0 then throw ProtocolException("controller state journal made no append progress")
      position += written

  private def readFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val read = channel.read(buffer, position)
      if read < 0 then throw ProtocolException("unexpected end of controller state journal")
      if read == 0 then throw ProtocolException("controller state journal made no read progress")
      position += read

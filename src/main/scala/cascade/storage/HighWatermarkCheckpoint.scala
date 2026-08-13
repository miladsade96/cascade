package cascade.storage

import cascade.protocol.ProtocolException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import java.util.zip.CRC32C

/** Fixed-size, double-buffered checkpoint for a partition's committed offset. */
private[storage] final class HighWatermarkCheckpoint(path: Path) extends AutoCloseable:
  private val Magic = 0x4348574d // CHWM
  private val FormatVersion: Short = 1
  private val SlotBytes = 32
  private val PayloadBytes = 24

  Option(path.getParent).foreach(directory => Files.createDirectories(directory): Unit)
  private val existedAtOpen = Files.exists(path)
  private val channel = FileChannel.open(
    path,
    StandardOpenOption.CREATE,
    StandardOpenOption.READ,
    StandardOpenOption.WRITE
  )
  private var closed = false
  private var current = recover()

  def offset: Option[Long] = synchronized(current.map(_._2))

  def existed: Boolean = existedAtOpen

  def persist(offset: Long): Unit = synchronized {
    ensureOpen()
    if offset < 0L then throw ProtocolException(s"negative high watermark: $offset")
    if !current.exists(_._2 == offset) then
      val generation = current.fold(0L)(entry => Math.addExact(entry._1, 1L))
      val slot = ByteBuffer.allocate(SlotBytes)
      slot.putInt(Magic)
      slot.putShort(FormatVersion)
      slot.putShort(0.toShort)
      slot.putLong(generation)
      slot.putLong(offset)
      val checksum = CRC32C()
      checksum.update(slot.array(), 0, PayloadBytes)
      slot.putInt(checksum.getValue.toInt)
      slot.putInt(0)
      slot.flip()
      writeFully(slot, Math.floorMod(generation, 2L) * SlotBytes.toLong)
      channel.force(false)
      current = Some((generation, offset))
  }

  override def close(): Unit = synchronized {
    if !closed then
      channel.force(false)
      channel.close()
      closed = true
  }

  private def recover(): Option[(Long, Long)] =
    val size = channel.size()
    Vector(0, 1).flatMap(index => readSlot(index, size)).sortBy(_._1).lastOption

  private def readSlot(index: Int, fileSize: Long): Option[(Long, Long)] =
    val position = index.toLong * SlotBytes.toLong
    if fileSize - position < SlotBytes.toLong then None
    else
      val bytes = new Array[Byte](SlotBytes)
      readFully(ByteBuffer.wrap(bytes), position)
      val buffer = ByteBuffer.wrap(bytes)
      val magic = buffer.getInt()
      val version = buffer.getShort()
      buffer.getShort()
      val generation = buffer.getLong()
      val offset = buffer.getLong()
      val expectedChecksum = buffer.getInt()
      val checksum = CRC32C()
      checksum.update(bytes, 0, PayloadBytes)
      Option.when(
        magic == Magic && version == FormatVersion && generation >= 0L && offset >= 0L &&
          checksum.getValue.toInt == expectedChecksum
      )((generation, offset))

  private def ensureOpen(): Unit =
    if closed then throw IllegalStateException("high-watermark checkpoint is closed")

  private def writeFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val written = channel.write(buffer, position)
      if written <= 0 then throw ProtocolException("high-watermark checkpoint made no write progress")
      position += written

  private def readFully(buffer: ByteBuffer, start: Long): Unit =
    var position = start
    while buffer.hasRemaining do
      val read = channel.read(buffer, position)
      if read < 0 then throw ProtocolException("unexpected end of high-watermark checkpoint")
      if read == 0 then throw ProtocolException("high-watermark checkpoint made no read progress")
      position += read

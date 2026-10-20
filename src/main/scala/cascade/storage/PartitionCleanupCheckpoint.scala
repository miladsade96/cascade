package cascade.storage

import cascade.protocol.ProtocolException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption
import java.nio.file.{Files, Path}
import java.util.zip.CRC32C

/** CRC32C-protected, atomically replaced replicated-cleanup state for one partition. */
private[storage] final class PartitionCleanupCheckpoint(path: Path):
  private val Magic = 0x43434c31 // CCL1
  private val FormatVersion: Short = 1
  private val HeaderBytes = 8
  private val DecisionBytes = 40
  private val ChecksumBytes = 4
  private val TotalBytes = HeaderBytes + DecisionBytes * 3 + ChecksumBytes
  private val temporary = path.resolveSibling(path.getFileName.toString + ".next")

  Option(path.getParent).foreach(directory => Files.createDirectories(directory): Unit)
  Files.deleteIfExists(temporary): Unit

  private var current = load()

  def recovered: PartitionCleanupState = synchronized(current)

  def persist(state: PartitionCleanupState): PartitionCleanupState = synchronized {
    if state != current then
      val buffer = ByteBuffer.allocate(TotalBytes)
      buffer.putInt(Magic)
      buffer.putShort(FormatVersion)
      buffer.putShort(0.toShort)
      encodeDecision(buffer, state.prepared)
      encodeDecision(buffer, state.committed)
      encodeDecision(buffer, state.applied)
      val checksum = CRC32C()
      checksum.update(buffer.array(), 0, TotalBytes - ChecksumBytes)
      buffer.putInt(checksum.getValue.toInt)
      buffer.flip()

      Files.deleteIfExists(temporary): Unit
      val channel = FileChannel.open(temporary, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
      try
        while buffer.hasRemaining do
          val written = channel.write(buffer)
          if written <= 0 then throw ProtocolException("cleanup checkpoint made no write progress")
        channel.force(true)
      finally channel.close()
      AtomicFileLifecycle.replace(temporary, path)
      current = state
    current
  }

  private def load(): PartitionCleanupState =
    if !Files.exists(path) then PartitionCleanupState.Empty
    else
      val bytes = Files.readAllBytes(path)
      if bytes.length != TotalBytes then throw ProtocolException("cleanup checkpoint length mismatch")
      val expectedChecksum = ByteBuffer.wrap(bytes).getInt(TotalBytes - ChecksumBytes)
      val checksum = CRC32C()
      checksum.update(bytes, 0, TotalBytes - ChecksumBytes)
      if expectedChecksum != checksum.getValue.toInt then
        throw ProtocolException("cleanup checkpoint CRC32C mismatch")
      val buffer = ByteBuffer.wrap(bytes)
      val magic = buffer.getInt()
      val version = buffer.getShort()
      val reserved = buffer.getShort()
      if magic != Magic || version != FormatVersion || reserved != 0 then
        throw ProtocolException("invalid cleanup checkpoint header")
      PartitionCleanupState(
        decodeDecision(buffer),
        decodeDecision(buffer),
        decodeDecision(buffer)
      )

  private def encodeDecision(buffer: ByteBuffer, value: Option[PartitionCleanupDecision]): Unit =
    value match
      case Some(decision) =>
        buffer.putInt(1)
        buffer.putInt(decision.leaderEpoch)
        buffer.putLong(decision.generation)
        buffer.putLong(decision.plan.deleteBeforeOffset)
        buffer.putLong(decision.plan.compactBeforeOffset)
        buffer.putLong(decision.plan.tombstoneCutoffMillis): Unit
      case None =>
        buffer.putInt(0)
        (0 until (DecisionBytes - Integer.BYTES)).foreach(_ => buffer.put(0.toByte))

  private def decodeDecision(buffer: ByteBuffer): Option[PartitionCleanupDecision] =
    buffer.getInt() match
      case 0 =>
        val reserved = new Array[Byte](DecisionBytes - Integer.BYTES)
        buffer.get(reserved)
        if reserved.exists(_ != 0.toByte) then throw ProtocolException("cleanup checkpoint empty slot is not zeroed")
        None
      case 1 =>
        Some(
          PartitionCleanupDecision(
            buffer.getInt(),
            buffer.getLong(),
            PartitionCleanupPlan(buffer.getLong(), buffer.getLong(), buffer.getLong())
          )
        )
      case _ => throw ProtocolException("cleanup checkpoint has invalid decision marker")

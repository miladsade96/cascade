package cascade.broker

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file.StandardOpenOption.{CREATE, TRUNCATE_EXISTING, WRITE}
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

enum RecoveryMode:
  case Fresh, Clean, Unclean

/** Forced marker that distinguishes a completed shutdown from a killed broker process. */
final class ShutdownMarker(dataDirectory: Path):
  private val MarkerBytes = ShutdownMarker.MarkerBytes
  private val internalDirectory = dataDirectory.resolve(".cascade")
  private val markerPath = internalDirectory.resolve("clean-shutdown.marker")
  private val temporaryPath = internalDirectory.resolve("clean-shutdown.marker.tmp")
  private val runningPath = internalDirectory.resolve("broker-running.marker")

  def beginRecovery(): RecoveryMode = synchronized {
    val hadPersistentData = containsPersistentData()
    val markerExists = Files.exists(markerPath)
    val wasRunning = Files.exists(runningPath)
    val clean = markerExists && Files.readAllBytes(markerPath).sameElements(MarkerBytes)
    Files.deleteIfExists(markerPath): Unit
    Files.deleteIfExists(temporaryPath): Unit
    Files.createDirectories(internalDirectory): Unit
    writeForced(runningPath)
    if wasRunning then RecoveryMode.Unclean
    else if clean then RecoveryMode.Clean
    else if markerExists || hadPersistentData then RecoveryMode.Unclean
    else RecoveryMode.Fresh
  }

  def markClean(): Unit = synchronized {
    Files.createDirectories(internalDirectory): Unit
    writeForced(temporaryPath)
    try Files.move(temporaryPath, markerPath, ATOMIC_MOVE, REPLACE_EXISTING): Unit
    catch case _: java.nio.file.AtomicMoveNotSupportedException =>
      Files.move(temporaryPath, markerPath, REPLACE_EXISTING): Unit
    Files.deleteIfExists(runningPath): Unit
  }

  private def writeForced(path: Path): Unit =
    val channel = FileChannel.open(path, CREATE, WRITE, TRUNCATE_EXISTING)
    try
      val buffer = ByteBuffer.wrap(MarkerBytes)
      while buffer.hasRemaining do
        if channel.write(buffer) <= 0 then throw IllegalStateException("clean-shutdown marker made no write progress")
      channel.force(true)
    finally channel.close()

  private def containsPersistentData(): Boolean =
    if !Files.exists(dataDirectory) then false
    else
      val paths = Files.walk(dataDirectory)
      try
        paths.iterator().asScala.exists(path =>
          Files.isRegularFile(path) && path != markerPath && path != temporaryPath && path != runningPath && Files.size(path) > 0L
        )
      finally paths.close()

object ShutdownMarker:
  private[cascade] val MarkerBytes = Array[Byte]('C', 'S', 'C', 'L', 1)

  def isCleanlyStopped(dataDirectory: Path): Boolean =
    val internalDirectory = dataDirectory.resolve(".cascade")
    val clean = internalDirectory.resolve("clean-shutdown.marker")
    val running = internalDirectory.resolve("broker-running.marker")
    Files.isRegularFile(clean) &&
      !Files.exists(running) &&
      Files.readAllBytes(clean).sameElements(MarkerBytes)

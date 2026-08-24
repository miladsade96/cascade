package cascade.storage

import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file.{AtomicMoveNotSupportedException, Files, Path}
import java.nio.channels.{ClosedChannelException, FileChannel}
import scala.jdk.CollectionConverters.*

/** Atomic rename protocols make interrupted deletion/replacement recoverable on startup. */
private[cascade] object AtomicFileLifecycle:
  private val DeletedSuffix = ".deleted"
  private val CleanedSuffix = ".cleaned"

  def markDeleted(path: Path): Path =
    val marked = path.resolveSibling(path.getFileName.toString + DeletedSuffix)
    move(path, marked, replace = true)
    marked

  def purgeMarked(path: Path): Unit = Files.deleteIfExists(path): Unit

  def recoverDeleted(directory: Path): Unit =
    if Files.isDirectory(directory) then
      val paths = Files.list(directory)
      try
        paths.iterator().asScala
          .filter(_.getFileName.toString.endsWith(DeletedSuffix))
          .foreach(path => Files.deleteIfExists(path): Unit)
      finally paths.close()

  def recoverReplacements(directory: Path): Unit =
    if Files.isDirectory(directory) then
      val paths = Files.list(directory)
      try
        paths.iterator().asScala
          .filter(_.getFileName.toString.endsWith(CleanedSuffix))
          .foreach { temporary =>
            val name = temporary.getFileName.toString.stripSuffix(CleanedSuffix)
            val target = temporary.resolveSibling(name)
            if Files.exists(target) then Files.deleteIfExists(temporary): Unit
            else replace(temporary, target)
          }
      finally paths.close()

  def replace(temp: Path, target: Path): Unit = move(temp, target, replace = true)

  /** An interrupted force closes its channel. Shutdown must still remain idempotent. */
  def forceAndClose(channel: FileChannel): Unit =
    if channel.isOpen then
      try channel.force(false)
      catch case _: ClosedChannelException => ()
      finally if channel.isOpen then channel.close()

  private def move(source: Path, target: Path, replace: Boolean): Unit =
    val options = if replace then Array(ATOMIC_MOVE, REPLACE_EXISTING) else Array(ATOMIC_MOVE)
    try Files.move(source, target, options*): Unit
    catch
      case _: AtomicMoveNotSupportedException =>
        if replace then Files.move(source, target, REPLACE_EXISTING): Unit
        else Files.move(source, target): Unit

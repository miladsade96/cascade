package cascade.storage

import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file.{AtomicMoveNotSupportedException, Files, Path}
import scala.jdk.CollectionConverters.*

/** Atomic rename protocols make interrupted deletion/replacement recoverable on startup. */
private[cascade] object AtomicFileLifecycle:
  private val DeletedSuffix = ".deleted"

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

  def replace(temp: Path, target: Path): Unit = move(temp, target, replace = true)

  private def move(source: Path, target: Path, replace: Boolean): Unit =
    val options = if replace then Array(ATOMIC_MOVE, REPLACE_EXISTING) else Array(ATOMIC_MOVE)
    try Files.move(source, target, options*): Unit
    catch
      case _: AtomicMoveNotSupportedException =>
        if replace then Files.move(source, target, REPLACE_EXISTING): Unit
        else Files.move(source, target): Unit

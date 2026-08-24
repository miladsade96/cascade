package cascade.backup

import java.io.IOException
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption.{READ, WRITE}
import java.nio.file.Path

private[backup] object BackupDurability:
  def forceFile(path: Path): Unit =
    val channel = FileChannel.open(path, WRITE)
    try channel.force(true)
    finally channel.close()

  def forceDirectoryWhenSupported(path: Path): Unit =
    try
      val channel = FileChannel.open(path, READ)
      try channel.force(true)
      finally channel.close()
    catch
      case _: UnsupportedOperationException => ()
      case _: IOException                   => ()


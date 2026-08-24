package cascade.backup

import java.nio.file.{Path, Paths}

object BackupCommand:
  def run(arguments: Array[String]): String = arguments.toList match
    case "backup" :: tail =>
      val options = parseOptions(tail)
      val dataDirectory = required(options, "--data-dir")
      val backupDirectory = required(options, "--backup-dir")
      val manifest = BackupCreator.create(dataDirectory, backupDirectory)
      s"Created backup with ${manifest.entries.size} files at ${backupDirectory.toAbsolutePath.normalize()}"
    case "verify-backup" :: tail =>
      val options = parseOptions(tail)
      val backupDirectory = required(options, "--backup-dir")
      val verified = BackupRestore.verify(backupDirectory)
      s"Verified ${verified.manifest.entries.size} files and ${verified.totalBytes} bytes at ${backupDirectory.toAbsolutePath.normalize()}"
    case "restore" :: tail =>
      val options = parseOptions(tail)
      val backupDirectory = required(options, "--backup-dir")
      val dataDirectory = required(options, "--data-dir")
      val restored = BackupRestore.restore(backupDirectory, dataDirectory)
      s"Restored ${restored.manifest.entries.size} files and ${restored.totalBytes} bytes to ${dataDirectory.toAbsolutePath.normalize()}"
    case command :: _ => throw IllegalArgumentException(s"unknown maintenance command: $command")
    case Nil => throw IllegalArgumentException("a maintenance command is required")

  private def parseOptions(arguments: List[String]): Map[String, Path] =
    @annotation.tailrec
    def loop(remaining: List[String], result: Map[String, Path]): Map[String, Path] = remaining match
      case Nil => result
      case option :: value :: tail if Set("--data-dir", "--backup-dir").contains(option) =>
        require(!result.contains(option), s"duplicate maintenance option: $option")
        loop(tail, result.updated(option, Paths.get(value)))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete maintenance option: $option")
    loop(arguments, Map.empty)

  private def required(options: Map[String, Path], name: String): Path =
    options.getOrElse(name, throw IllegalArgumentException(s"missing maintenance option: $name"))


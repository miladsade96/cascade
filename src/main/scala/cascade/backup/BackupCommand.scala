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
    case "verify-cluster-snapshot" :: tail =>
      val options = parseOptions(tail)
      val snapshotDirectory = required(options, "--snapshot-dir")
      val verified = ClusterSnapshotRepository.verify(snapshotDirectory)
      s"Verified cluster snapshot ${verified.manifest.identity.snapshotId} with ${verified.manifest.nodes.size} nodes and ${verified.totalBytes} bytes at ${snapshotDirectory.toAbsolutePath.normalize()}"
    case "prune-cluster-snapshots" :: tail =>
      val options = parseOptions(tail)
      val repository = required(options, "--repository-dir")
      val retain = requiredValue(options, "--retain").toInt
      val removed = ClusterSnapshotRepository.prune(repository, retain)
      s"Removed ${removed.size} verified cluster snapshot generations from ${repository.toAbsolutePath.normalize()}"
    case command :: _ => throw IllegalArgumentException(s"unknown maintenance command: $command")
    case Nil => throw IllegalArgumentException("a maintenance command is required")

  private def parseOptions(arguments: List[String]): Map[String, String] =
    @annotation.tailrec
    def loop(remaining: List[String], result: Map[String, String]): Map[String, String] = remaining match
      case Nil => result
      case option :: value :: tail if Set(
          "--data-dir", "--backup-dir", "--snapshot-dir", "--repository-dir", "--retain"
        ).contains(option) =>
        require(!result.contains(option), s"duplicate maintenance option: $option")
        loop(tail, result.updated(option, value))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete maintenance option: $option")
    loop(arguments, Map.empty)

  private def required(options: Map[String, String], name: String): Path = Paths.get(requiredValue(options, name))

  private def requiredValue(options: Map[String, String], name: String): String =
    options.getOrElse(name, throw IllegalArgumentException(s"missing maintenance option: $name"))


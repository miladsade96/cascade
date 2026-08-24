package cascade.backup

import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, COPY_ATTRIBUTES}
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.jdk.CollectionConverters.*

final case class BackupVerification(manifest: BackupManifest, totalBytes: Long)

object BackupRestore:
  def verify(backupDirectory: Path): BackupVerification =
    val backup = backupDirectory.toAbsolutePath.normalize()
    require(Files.isDirectory(backup), s"backup is not a directory: $backup")
    val manifestPath = backup.resolve(BackupManifest.FileName)
    require(Files.isRegularFile(manifestPath) && !Files.isSymbolicLink(manifestPath), "backup manifest is missing or unsafe")
    val manifest = BackupManifest.read(manifestPath)
    val expected = manifest.entries.map(_.relativePath).toSet
    val actual = backupFiles(backup).filterNot(_ == BackupManifest.FileName).toSet
    require(actual == expected, "backup contents do not match the manifest")
    manifest.entries.foreach { entry =>
      val path = entry.resolveUnder(backup)
      require(Files.size(path) == entry.length, s"backup length mismatch: ${entry.relativePath}")
      require(Sha256.file(path) == entry.sha256, s"backup checksum mismatch: ${entry.relativePath}")
    }
    BackupVerification(manifest, manifest.entries.map(_.length).sum)

  def restore(backupDirectory: Path, targetDirectory: Path): BackupVerification =
    val backup = backupDirectory.toAbsolutePath.normalize()
    val target = targetDirectory.toAbsolutePath.normalize()
    require(!Files.exists(target), s"restore target already exists: $target")
    require(backup != target && !target.startsWith(backup), "restore target must be outside the backup directory")
    val verified = verify(backup)
    val parent = Option(target.getParent).getOrElse(throw IllegalArgumentException("restore target requires a parent directory"))
    Files.createDirectories(parent): Unit
    val prefix = s".${target.getFileName}.restore-"
    val staging = parent.resolve(prefix + UUID.randomUUID()).toAbsolutePath.normalize()
    require(staging.getParent == parent && staging.getFileName.toString.startsWith(prefix), "invalid restore staging path")
    Files.createDirectory(staging): Unit
    var published = false
    try
      verified.manifest.entries.foreach { entry =>
        val source = entry.resolveUnder(backup)
        val destination = entry.resolveUnder(staging)
        Option(destination.getParent).foreach(directory => Files.createDirectories(directory): Unit)
        Files.copy(source, destination, COPY_ATTRIBUTES): Unit
        require(Files.size(destination) == entry.length, s"restored length mismatch: ${entry.relativePath}")
        require(Sha256.file(destination) == entry.sha256, s"restored checksum mismatch: ${entry.relativePath}")
      }
      require(verify(backup) == verified, "backup changed while it was being restored")
      Files.move(staging, target, ATOMIC_MOVE): Unit
      published = true
      verified
    finally
      if !published then cleanupStaging(staging, parent, prefix)

  private def backupFiles(backup: Path): Vector[String] =
    val paths = Files.walk(backup)
    try
      paths.iterator().asScala.filter(_ != backup).flatMap { path =>
        require(!Files.isSymbolicLink(path), s"symbolic links are not supported in backups: $path")
        if Files.isRegularFile(path) then
          Some(backup.relativize(path).iterator().asScala.map(_.toString).mkString("/"))
        else
          require(Files.isDirectory(path), s"unsupported backup entry: $path")
          None
      }.toVector.sorted
    finally paths.close()

  private def cleanupStaging(staging: Path, parent: Path, prefix: String): Unit =
    require(staging.getParent == parent && staging.getFileName.toString.startsWith(prefix), "refusing unsafe restore cleanup")
    if Files.exists(staging) then
      val paths = Files.walk(staging)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(path => Files.deleteIfExists(path): Unit)
      finally paths.close()


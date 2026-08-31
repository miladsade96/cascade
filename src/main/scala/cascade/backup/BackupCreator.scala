package cascade.backup

import cascade.broker.ShutdownMarker
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, COPY_ATTRIBUTES}
import java.nio.file.StandardOpenOption.{CREATE_NEW, WRITE}
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.UUID
import scala.jdk.CollectionConverters.*

object BackupCreator:
  private final case class SourceFile(path: Path, relativePath: String, length: Long, modified: FileTime)

  def create(sourceDirectory: Path, targetDirectory: Path, clock: () => Instant = () => Instant.now()): BackupManifest =
    createConsistent(sourceDirectory, targetDirectory, requireCleanShutdown = true, clock)

  /** Caller must hold the broker write barrier and force every partition before invoking this method. */
  def createOnline(sourceDirectory: Path, targetDirectory: Path, clock: () => Instant = () => Instant.now()): BackupManifest =
    createConsistent(sourceDirectory, targetDirectory, requireCleanShutdown = false, clock)

  private def createConsistent(
      sourceDirectory: Path,
      targetDirectory: Path,
      requireCleanShutdown: Boolean,
      clock: () => Instant
  ): BackupManifest =
    val source = sourceDirectory.toAbsolutePath.normalize()
    val target = targetDirectory.toAbsolutePath.normalize()
    require(Files.isDirectory(source), s"backup source is not a directory: $source")
    require(!Files.exists(target), s"backup target already exists: $target")
    require(source != target && !target.startsWith(source), "backup target must be outside the source directory")
    if requireCleanShutdown then require(ShutdownMarker.isCleanlyStopped(source), "backup requires a cleanly stopped broker")
    val parent = Option(target.getParent).getOrElse(throw IllegalArgumentException("backup target requires a parent directory"))
    Files.createDirectories(parent): Unit
    val prefix = s".${target.getFileName}.partial-"
    val staging = parent.resolve(prefix + UUID.randomUUID()).toAbsolutePath.normalize()
    require(staging.getParent == parent && staging.getFileName.toString.startsWith(prefix), "invalid backup staging path")
    Files.createDirectory(staging): Unit
    var published = false
    try
      val before = sourceFiles(source)
      val entries = before.map { file =>
        val destination = BackupEntry(file.relativePath, file.length, "0" * 64).resolveUnder(staging)
        Option(destination.getParent).foreach(directory => Files.createDirectories(directory): Unit)
        Files.copy(file.path, destination, COPY_ATTRIBUTES): Unit
        val copiedLength = Files.size(destination)
        require(copiedLength == file.length, s"file changed while copying: ${file.relativePath}")
        BackupDurability.forceFile(destination)
        BackupEntry(file.relativePath, copiedLength, Sha256.file(destination))
      }
      if requireCleanShutdown then require(ShutdownMarker.isCleanlyStopped(source), "broker started while the backup was running")
      require(sourceFiles(source) == before, "backup source changed while it was being copied")
      val manifest = BackupManifest(clock(), entries)
      writeForced(staging.resolve(BackupManifest.FileName), BackupManifest.encode(manifest))
      BackupDurability.forceDirectoryWhenSupported(staging)
      Files.move(staging, target, ATOMIC_MOVE): Unit
      BackupDurability.forceDirectoryWhenSupported(parent)
      published = true
      manifest
    finally
      if !published then cleanupStaging(staging, parent, prefix)

  private def sourceFiles(source: Path): Vector[SourceFile] =
    val paths = Files.walk(source)
    try
      paths.iterator().asScala.filter(_ != source).flatMap { path =>
        require(!Files.isSymbolicLink(path), s"symbolic links are not supported in backups: $path")
        if Files.isRegularFile(path) then
          val relative = source.relativize(path).iterator().asScala.map(_.toString).mkString("/")
          Some(SourceFile(path, relative, Files.size(path), Files.getLastModifiedTime(path)))
        else
          require(Files.isDirectory(path), s"unsupported backup source entry: $path")
          None
      }.toVector.sortBy(_.relativePath)
    finally paths.close()

  private def writeForced(path: Path, value: String): Unit =
    val channel = FileChannel.open(path, CREATE_NEW, WRITE)
    try
      val buffer = ByteBuffer.wrap(value.getBytes(StandardCharsets.UTF_8))
      while buffer.hasRemaining do
        if channel.write(buffer) <= 0 then throw IllegalStateException("backup manifest made no write progress")
      channel.force(true)
    finally channel.close()

  private def cleanupStaging(staging: Path, parent: Path, prefix: String): Unit =
    require(staging.getParent == parent && staging.getFileName.toString.startsWith(prefix), "refusing unsafe staging cleanup")
    if Files.exists(staging) then
      val paths = Files.walk(staging)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(path => Files.deleteIfExists(path): Unit)
      finally paths.close()

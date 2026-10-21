package cascade.backup

import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

object ClusterSnapshotRepository:
  def verify(snapshotDirectory: Path): ClusterSnapshotVerification =
    val root = snapshotDirectory.toAbsolutePath.normalize()
    require(Files.isDirectory(root) && !Files.isSymbolicLink(root), s"cluster snapshot is not a safe directory: $root")
    val manifestPath = root.resolve(ClusterSnapshotManifestCodec.FileName)
    require(Files.isRegularFile(manifestPath) && !Files.isSymbolicLink(manifestPath), "cluster snapshot manifest is missing or unsafe")
    val manifest = ClusterSnapshotManifestCodec.read(manifestPath)
    val expected = manifest.nodes.map(_.artifactDirectory).toSet + ClusterSnapshotManifestCodec.FileName
    val entries = Files.list(root)
    val actual = try entries.iterator().asScala.map(_.getFileName.toString).toSet finally entries.close()
    require(actual == expected, "cluster snapshot contents do not match the cluster manifest")
    var totalBytes = 0L
    manifest.nodes.foreach { artifact =>
      val directory = root.resolve(artifact.artifactDirectory).normalize()
      require(directory.getParent == root, "cluster snapshot artifact escapes its generation")
      val verified = BackupRestore.verify(directory)
      require(verified.manifest.entries.size == artifact.fileCount, s"snapshot file count mismatch for node ${artifact.nodeId}")
      require(verified.totalBytes == artifact.totalBytes, s"snapshot byte count mismatch for node ${artifact.nodeId}")
      require(
        Sha256.file(directory.resolve(BackupManifest.FileName)) == artifact.manifestSha256,
        s"snapshot manifest checksum mismatch for node ${artifact.nodeId}"
      )
      totalBytes = Math.addExact(totalBytes, verified.totalBytes)
    }
    ClusterSnapshotVerification(manifest, totalBytes)

  /** Deletes only fully verified generations that have the requested number of newer verified replacements. */
  def prune(repositoryDirectory: Path, retain: Int): Vector[Path] =
    require(retain >= 1, "cluster snapshot retention must keep at least one verified generation")
    val repository = repositoryDirectory.toAbsolutePath.normalize()
    require(Files.isDirectory(repository) && !Files.isSymbolicLink(repository), s"snapshot repository is not a safe directory: $repository")
    val children = Files.list(repository)
    val candidates = try
      children.iterator().asScala.filter { path =>
        Files.isDirectory(path) && !Files.isSymbolicLink(path) &&
          Files.exists(path.resolve(ClusterSnapshotManifestCodec.FileName))
      }.toVector
    finally children.close()
    val verified = candidates.map(path => path -> verify(path)).sortBy { case (_, result) =>
      (result.manifest.createdAt, result.manifest.identity.snapshotId.toString)
    }.reverse
    val removable = verified.drop(retain).map(_._1)
    removable.foreach(path => deleteGeneration(repository, path))
    removable

  private def deleteGeneration(repository: Path, generation: Path): Unit =
    val target = generation.toAbsolutePath.normalize()
    require(target.getParent == repository && target != repository, "refusing unsafe snapshot generation deletion")
    val paths = Files.walk(target)
    try
      val ordered = paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse
      ordered.foreach { path =>
        require(path == target || path.startsWith(target), "snapshot deletion escaped its generation")
        require(!Files.isSymbolicLink(path), s"refusing to delete symbolic link in snapshot generation: $path")
        Files.delete(path): Unit
      }
    finally paths.close()

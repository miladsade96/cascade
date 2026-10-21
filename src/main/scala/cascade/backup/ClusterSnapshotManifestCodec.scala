package cascade.backup

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, StandardCopyOption, StandardOpenOption}
import java.time.Instant
import java.util.{Base64, UUID}

object ClusterSnapshotManifestCodec:
  val FileName = "cascade-cluster-snapshot.manifest"
  private val Header = "CASCADE-CLUSTER-SNAPSHOT\t1"
  private val encoder = Base64.getUrlEncoder.withoutPadding()
  private val decoder = Base64.getUrlDecoder

  def encode(manifest: ClusterSnapshotManifest): String =
    val identity = manifest.identity
    val body = manifest.nodes.sortBy(_.nodeId).flatMap { node =>
      val nodeLine = Vector(
        "node",
        node.nodeId.toString,
        node.directoryId,
        encodeText(node.artifactDirectory),
        node.manifestSha256,
        node.fileCount.toString,
        node.totalBytes.toString,
        node.watermarks.size.toString
      ).mkString("\t")
      nodeLine +: node.watermarks.sortBy(value => value.topic -> value.partition).map { watermark =>
        Vector(
          "watermark",
          node.nodeId.toString,
          encodeText(watermark.topic),
          watermark.partition.toString,
          watermark.highWatermark.toString
        ).mkString("\t")
      }
    }
    (Vector(
      Header,
      s"snapshot_id\t${identity.snapshotId}",
      s"created_at\t${manifest.createdAt}",
      s"controller_id\t${identity.controllerId}",
      s"controller_term\t${identity.controllerTerm}",
      s"metadata_version\t${identity.metadataVersion}",
      s"metadata_sha256\t${identity.metadataSha256}",
      s"node_count\t${manifest.nodes.size}"
    ) ++ body).mkString("", "\n", "\n")

  def decode(value: String): ClusterSnapshotManifest =
    val lines = value.linesIterator.toVector
    require(lines.length >= 9, "cluster snapshot manifest is incomplete")
    require(lines.head == Header, "unsupported cluster snapshot manifest format")
    val snapshotId = UUID.fromString(field(lines(1), "snapshot_id"))
    val createdAt = Instant.parse(field(lines(2), "created_at"))
    val controllerId = field(lines(3), "controller_id").toInt
    val controllerTerm = field(lines(4), "controller_term").toLong
    val metadataVersion = field(lines(5), "metadata_version").toLong
    val metadataSha256 = field(lines(6), "metadata_sha256")
    val nodeCount = field(lines(7), "node_count").toInt
    require(nodeCount > 0, "cluster snapshot node count must be positive")
    var index = 8
    val nodes = Vector.newBuilder[NodeSnapshotArtifact]
    (0 until nodeCount).foreach { _ =>
      require(index < lines.length, "cluster snapshot node entry is missing")
      val parts = lines(index).split("\\t", -1).toVector
      require(parts.size == 8 && parts.head == "node", "cluster snapshot node entry is invalid")
      val nodeId = parts(1).toInt
      val watermarkCount = parts(7).toInt
      require(watermarkCount >= 0, "cluster snapshot watermark count must be non-negative")
      index += 1
      val watermarks = Vector.newBuilder[SnapshotWatermark]
      (0 until watermarkCount).foreach { _ =>
        require(index < lines.length, "cluster snapshot watermark entry is missing")
        val watermark = lines(index).split("\\t", -1).toVector
        require(
          watermark.size == 5 && watermark.head == "watermark" && watermark(1).toInt == nodeId,
          "cluster snapshot watermark entry is invalid"
        )
        watermarks += SnapshotWatermark(decodeText(watermark(2)), watermark(3).toInt, watermark(4).toLong)
        index += 1
      }
      nodes += NodeSnapshotArtifact(
        nodeId,
        parts(2),
        decodeText(parts(3)),
        parts(4),
        parts(5).toInt,
        parts(6).toLong,
        watermarks.result()
      )
    }
    require(index == lines.length, "cluster snapshot manifest contains trailing entries")
    ClusterSnapshotManifest(
      ClusterSnapshotIdentity(snapshotId, controllerId, controllerTerm, metadataVersion, metadataSha256),
      createdAt,
      nodes.result()
    )

  def read(path: Path): ClusterSnapshotManifest = decode(Files.readString(path, StandardCharsets.UTF_8))

  def writeAtomic(directory: Path, manifest: ClusterSnapshotManifest): Path =
    val root = directory.toAbsolutePath.normalize()
    Files.createDirectories(root): Unit
    val target = root.resolve(FileName)
    require(!Files.exists(target), s"cluster snapshot manifest already exists: $target")
    val temporary = root.resolve(s".$FileName.${manifest.identity.snapshotId}.partial")
    require(!Files.exists(temporary), s"cluster snapshot staging manifest already exists: $temporary")
    try
      Files.writeString(temporary, encode(manifest), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE): Unit
      BackupDurability.forceFile(temporary)
      Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE): Unit
      BackupDurability.forceDirectoryWhenSupported(root)
      target
    finally Files.deleteIfExists(temporary): Unit

  private def field(line: String, expected: String): String =
    line.split("\\t", -1).toVector match
      case Vector(name, content) if name == expected => content
      case _ => throw IllegalArgumentException(s"cluster snapshot manifest field '$expected' is invalid")

  private def encodeText(value: String): String =
    encoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))

  private def decodeText(value: String): String =
    String(decoder.decode(value), StandardCharsets.UTF_8)

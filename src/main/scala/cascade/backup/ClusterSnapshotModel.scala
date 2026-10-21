package cascade.backup

import java.time.Instant
import java.util.UUID

final case class ClusterSnapshotIdentity(
    snapshotId: UUID,
    controllerId: Int,
    controllerTerm: Long,
    metadataVersion: Long,
    metadataSha256: String
):
  require(controllerId >= 0, "snapshot controller ID must be non-negative")
  require(controllerTerm >= 0L, "snapshot controller term must be non-negative")
  require(metadataVersion >= 0L, "snapshot metadata version must be non-negative")
  require(metadataSha256.matches("[0-9a-f]{64}"), "snapshot metadata SHA-256 must be lowercase hexadecimal")

final case class SnapshotWatermark(topic: String, partition: Int, highWatermark: Long):
  require(topic.nonEmpty, "snapshot watermark topic must not be empty")
  require(partition >= 0, "snapshot watermark partition must be non-negative")
  require(highWatermark >= 0L, "snapshot high watermark must be non-negative")

final case class NodeSnapshotArtifact(
    nodeId: Int,
    directoryId: String,
    artifactDirectory: String,
    manifestSha256: String,
    fileCount: Int,
    totalBytes: Long,
    watermarks: Vector[SnapshotWatermark]
):
  require(nodeId >= 0, "snapshot node ID must be non-negative")
  private val parsedDirectoryId = UUID.fromString(directoryId)
  require(parsedDirectoryId.getMostSignificantBits != 0L || parsedDirectoryId.getLeastSignificantBits != 0L,
    "snapshot directory ID must not be zero")
  require(artifactDirectory == s"node-$nodeId", "snapshot artifact directory must be bound to its node ID")
  require(manifestSha256.matches("[0-9a-f]{64}"), "snapshot manifest SHA-256 must be lowercase hexadecimal")
  require(fileCount >= 0, "snapshot file count must be non-negative")
  require(totalBytes >= 0L, "snapshot byte count must be non-negative")
  require(
    watermarks.map(value => value.topic -> value.partition).distinct.size == watermarks.size,
    "snapshot watermarks must be unique"
  )

final case class ClusterSnapshotManifest(
    identity: ClusterSnapshotIdentity,
    createdAt: Instant,
    nodes: Vector[NodeSnapshotArtifact]
):
  require(nodes.nonEmpty, "cluster snapshot must contain at least one node artifact")
  require(nodes.map(_.nodeId).distinct.size == nodes.size, "cluster snapshot node IDs must be unique")
  require(nodes.map(_.directoryId).distinct.size == nodes.size, "cluster snapshot directory IDs must be unique")
  require(nodes.exists(_.nodeId == identity.controllerId), "cluster snapshot must include its controller")

final case class ClusterSnapshotVerification(manifest: ClusterSnapshotManifest, totalBytes: Long)

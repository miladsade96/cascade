package cascade.backup

import cascade.protocol.{ByteCursor, ByteWriter}
import java.util.UUID

private[cascade] final case class SnapshotPrepareRequest(identity: ClusterSnapshotIdentity, leaseMillis: Long):
  require(leaseMillis > 0L, "snapshot prepare lease must be positive")

private[cascade] final case class SnapshotCaptureRequest(identity: ClusterSnapshotIdentity, targetDirectory: String):
  require(targetDirectory.nonEmpty, "snapshot target directory must not be empty")

private[cascade] object ClusterSnapshotWireCodec:
  def encodePrepare(request: SnapshotPrepareRequest): Array[Byte] =
    writeIdentity(ByteWriter(), request.identity).writeLong(request.leaseMillis).result()

  def decodePrepare(cursor: ByteCursor): SnapshotPrepareRequest =
    val request = SnapshotPrepareRequest(readIdentity(cursor), cursor.readLong())
    cursor.ensureFullyRead()
    request

  def encodeCapture(request: SnapshotCaptureRequest): Array[Byte] =
    writeIdentity(ByteWriter(), request.identity).writeString(request.targetDirectory).result()

  def decodeCapture(cursor: ByteCursor): SnapshotCaptureRequest =
    val request = SnapshotCaptureRequest(readIdentity(cursor), cursor.readString())
    cursor.ensureFullyRead()
    request

  def encodeRelease(snapshotId: UUID): Array[Byte] =
    ByteWriter().writeUuid(snapshotId.getMostSignificantBits, snapshotId.getLeastSignificantBits).result()

  def decodeRelease(cursor: ByteCursor): UUID =
    val (most, least) = cursor.readUuid()
    cursor.ensureFullyRead()
    UUID(most, least)

  def encodeArtifact(artifact: NodeSnapshotArtifact): Array[Byte] =
    val writer = ByteWriter()
    writer
      .writeInt(artifact.nodeId)
      .writeString(artifact.directoryId)
      .writeString(artifact.artifactDirectory)
      .writeString(artifact.manifestSha256)
      .writeInt(artifact.fileCount)
      .writeLong(artifact.totalBytes)
      .writeArray(artifact.watermarks) { watermark =>
        writer
          .writeString(watermark.topic)
          .writeInt(watermark.partition)
          .writeLong(watermark.highWatermark): Unit
      }
      .result()

  def decodeArtifact(cursor: ByteCursor): NodeSnapshotArtifact =
    val artifact = NodeSnapshotArtifact(
      cursor.readInt(),
      cursor.readString(),
      cursor.readString(),
      cursor.readString(),
      cursor.readInt(),
      cursor.readLong(),
      cursor.readArray(SnapshotWatermark(cursor.readString(), cursor.readInt(), cursor.readLong()))
    )
    cursor.ensureFullyRead()
    artifact

  private def writeIdentity(writer: ByteWriter, identity: ClusterSnapshotIdentity): ByteWriter =
    writer
      .writeUuid(identity.snapshotId.getMostSignificantBits, identity.snapshotId.getLeastSignificantBits)
      .writeInt(identity.controllerId)
      .writeLong(identity.controllerTerm)
      .writeLong(identity.metadataVersion)
      .writeString(identity.metadataSha256)

  private def readIdentity(cursor: ByteCursor): ClusterSnapshotIdentity =
    val (most, least) = cursor.readUuid()
    ClusterSnapshotIdentity(UUID(most, least), cursor.readInt(), cursor.readLong(), cursor.readLong(), cursor.readString())


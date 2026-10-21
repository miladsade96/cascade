package cascade.backup

import cascade.cluster.ClusterSnapshotPosition
import cascade.protocol.{ByteCursor, ByteWriter, Errors}
import java.nio.file.{Path, Paths}

private[cascade] final case class LocalSnapshotContent(
    manifestSha256: String,
    fileCount: Int,
    totalBytes: Long,
    watermarks: Vector[SnapshotWatermark]
)

private[cascade] final class ClusterSnapshotParticipant(
    nodeId: Int,
    position: () => ClusterSnapshotPosition,
    barrier: SnapshotRequestBarrier,
    captureLocal: Path => LocalSnapshotContent
):
  def handle(apiKey: Short, cursor: ByteCursor): Array[Byte] = apiKey match
    case cascade.cluster.InternalApi.SnapshotPrepare =>
      val request = ClusterSnapshotWireCodec.decodePrepare(cursor)
      prepare(request)
      ByteWriter().writeShort(Errors.None).result()
    case cascade.cluster.InternalApi.SnapshotCapture =>
      val request = ClusterSnapshotWireCodec.decodeCapture(cursor)
      val artifact = capture(request)
      ByteWriter().writeShort(Errors.None).writeBytes(ClusterSnapshotWireCodec.encodeArtifact(artifact)).result()
    case cascade.cluster.InternalApi.SnapshotRelease =>
      release(ClusterSnapshotWireCodec.decodeRelease(cursor))
      ByteWriter().writeShort(Errors.None).result()
    case _ => throw IllegalArgumentException(s"unsupported snapshot API: $apiKey")

  def prepare(request: SnapshotPrepareRequest): Unit =
    validate(request.identity)
    barrier.freeze(request.identity.snapshotId, request.leaseMillis)
    try validate(request.identity): Unit
    catch
      case error: Throwable =>
        barrier.release(request.identity.snapshotId)
        throw error

  def capture(request: SnapshotCaptureRequest): NodeSnapshotArtifact =
    validate(request.identity)
    val target = Paths.get(request.targetDirectory).toAbsolutePath.normalize()
    require(Option(target.getFileName).exists(_.toString == s"node-$nodeId"), "snapshot target must match the local node ID")
    val captureLease = barrier.beginCapture(request.identity.snapshotId)
    try
      val before = validate(request.identity)
      val content = captureLocal(target)
      val after = validate(request.identity)
      require(after == before, "cluster position changed while the node snapshot was captured")
      val voter = after.membership.voters.find(_.id == nodeId).getOrElse(
        throw IllegalStateException(s"node $nodeId is not in the snapshot voter set")
      )
      NodeSnapshotArtifact(
        nodeId,
        voter.directoryId.toString,
        s"node-$nodeId",
        content.manifestSha256,
        content.fileCount,
        content.totalBytes,
        content.watermarks,
      )
    finally captureLease.close()

  def release(snapshotId: java.util.UUID): Unit =
    if barrier.isOwnedBy(snapshotId) then barrier.release(snapshotId)

  private def validate(identity: ClusterSnapshotIdentity): ClusterSnapshotPosition =
    val current = position()
    require(!current.membership.isJoint, "cluster snapshot requires stable voter membership")
    require(current.unavailableBrokerIds.isEmpty, "cluster snapshot requires every voter to be available")
    require(current.membership.contains(nodeId), s"node $nodeId is not a current voter")
    require(current.controllerId == identity.controllerId, "snapshot controller changed")
    require(current.controllerTerm == identity.controllerTerm, "snapshot controller term changed")
    require(current.metadataVersion == identity.metadataVersion, "snapshot metadata version changed")
    require(current.metadataSha256 == identity.metadataSha256, "snapshot metadata fingerprint changed")
    current

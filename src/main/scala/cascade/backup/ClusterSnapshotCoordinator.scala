package cascade.backup

import cascade.cluster.{ClusterFeature, ClusterManager, ClusterNode, ClusterSnapshotPosition, InternalApi, PeerTransport}
import cascade.protocol.{ByteCursor, Errors, ProtocolException}
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.UUID
import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.util.{Failure, Success, Try}

private[cascade] final class ClusterSnapshotCoordinator(
    nodeId: Int,
    cluster: ClusterManager,
    peers: PeerTransport,
    participant: ClusterSnapshotParticipant,
    captureTimeoutMillis: Int,
    clock: () => Instant = () => Instant.now(),
    nextId: () => UUID = () => UUID.randomUUID()
) extends AutoCloseable:
  require(captureTimeoutMillis > 0, "cluster snapshot timeout must be positive")
  private val executor = Executors.newVirtualThreadPerTaskExecutor()

  def create(targetDirectory: Path): ClusterSnapshotManifest = synchronized {
    val target = targetDirectory.toAbsolutePath.normalize()
    require(!Files.exists(target.resolve(ClusterSnapshotManifestCodec.FileName)), "cluster snapshot is already published")
    val position = eligiblePosition()
    val identity = ClusterSnapshotIdentity(
      nextId(), position.controllerId, position.controllerTerm, position.metadataVersion, position.metadataSha256
    )
    val voters = position.membership.currentVoters.sortBy(_.id)
    val leaseMillis = math.max(60_000L, Math.multiplyExact(captureTimeoutMillis.toLong, 2L))
    var prepared = Vector.empty[ClusterNode]
    var primaryFailure: Throwable | Null = null
    try
      voters.foreach { voter =>
        prepare(voter.node, SnapshotPrepareRequest(identity, leaseMillis))
        prepared :+= voter.node
      }
      val futures = voters.map { voter =>
        executor.submit(new Callable[NodeSnapshotArtifact]:
          override def call(): NodeSnapshotArtifact = capture(
            voter.node,
            SnapshotCaptureRequest(identity, target.resolve(s"node-${voter.id}").toString)
          )
        )
      }
      val outcomes = futures.map(future => Try(future.get(captureTimeoutMillis.toLong, TimeUnit.MILLISECONDS)))
      outcomes.collectFirst { case Failure(error) => error }.foreach(error => throw error)
      val artifacts = outcomes.collect { case Success(artifact) => artifact }
      validateArtifacts(position, artifacts)
      require(cluster.clusterSnapshotPosition == position, "cluster position changed before snapshot publication")
      val manifest = ClusterSnapshotManifest(identity, clock(), artifacts)
      ClusterSnapshotManifestCodec.writeAtomic(target, manifest)
      manifest
    catch
      case error: Throwable =>
        primaryFailure = error
        throw error
    finally
      val releaseFailures = prepared.flatMap { node =>
        try
          release(node, identity.snapshotId)
          None
        catch case error: Throwable => Some(node.id -> error)
      }
      if releaseFailures.nonEmpty && primaryFailure == null then
        throw IllegalStateException(
          s"cluster snapshot was published but barriers could not be released on nodes ${releaseFailures.map(_._1).mkString(",")}",
          releaseFailures.head._2
        )
  }

  override def close(): Unit = executor.close()

  private def eligiblePosition(): ClusterSnapshotPosition =
    require(cluster.supportsFeature(ClusterFeature.ClusterSnapshot), "cluster snapshot feature is not active on every voter")
    val position = cluster.clusterSnapshotPosition
    require(position.controllerId == nodeId, s"cluster snapshot must run on controller ${position.controllerId}")
    require(!position.membership.isJoint, "cluster snapshot requires stable voter membership")
    require(position.unavailableBrokerIds.isEmpty, "cluster snapshot requires every voter to be available")
    require(position.membership.contains(nodeId), "cluster snapshot controller is not a voter")
    position

  private def prepare(node: ClusterNode, request: SnapshotPrepareRequest): Unit =
    if node.id == nodeId then participant.prepare(request)
    else
      val response = peers.call(node, InternalApi.SnapshotPrepare, ClusterSnapshotWireCodec.encodePrepare(request), captureTimeoutMillis)
      requireSuccess(response, "snapshot prepare")

  private def capture(node: ClusterNode, request: SnapshotCaptureRequest): NodeSnapshotArtifact =
    if node.id == nodeId then participant.capture(request)
    else
      val response = peers.call(node, InternalApi.SnapshotCapture, ClusterSnapshotWireCodec.encodeCapture(request), captureTimeoutMillis)
      val error = response.readShort()
      if error != Errors.None then throw ProtocolException(s"snapshot capture failed with error $error")
      ClusterSnapshotWireCodec.decodeArtifact(response)

  private def release(node: ClusterNode, snapshotId: UUID): Unit =
    if node.id == nodeId then participant.release(snapshotId)
    else
      val response = peers.call(
        node, InternalApi.SnapshotRelease, ClusterSnapshotWireCodec.encodeRelease(snapshotId), captureTimeoutMillis
      )
      requireSuccess(response, "snapshot release")

  private def requireSuccess(response: ByteCursor, operation: String): Unit =
    val error = response.readShort()
    response.ensureFullyRead()
    if error != Errors.None then throw ProtocolException(s"$operation failed with error $error")

  private def validateArtifacts(position: ClusterSnapshotPosition, artifacts: Vector[NodeSnapshotArtifact]): Unit =
    val expectedVoters = position.membership.currentVoters.map(voter => voter.id -> voter).toMap
    require(artifacts.map(_.nodeId).toSet == expectedVoters.keySet, "cluster snapshot did not capture every voter exactly once")
    artifacts.foreach { artifact =>
      require(
        expectedVoters(artifact.nodeId).directoryId.toString == artifact.directoryId,
        s"snapshot directory ID mismatch for node ${artifact.nodeId}"
      )
    }
    val watermarks = artifacts.map(artifact => artifact.nodeId -> artifact.watermarks.map(
      value => (value.topic, value.partition) -> value.highWatermark
    ).toMap).toMap
    position.topics.foreach { topic =>
      topic.partitions.foreach { partition =>
        val values = partition.inSyncReplicas.map { replicaId =>
          require(watermarks.contains(replicaId), s"snapshot artifact is missing for ISR node $replicaId")
          watermarks(replicaId).getOrElse(
            topic.name -> partition.partition,
            throw IllegalArgumentException(s"snapshot watermark is missing for ${topic.name}-${partition.partition} on node $replicaId")
          )
        }
        require(values.distinct.size <= 1, s"snapshot high watermarks diverged for ${topic.name}-${partition.partition}")
      }
    }

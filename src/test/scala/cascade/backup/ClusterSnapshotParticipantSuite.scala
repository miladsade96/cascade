package cascade.backup

import cascade.cluster.{ClusterNode, ClusterSnapshotPosition, QuorumMembership, QuorumVoter, VoterDirectoryId}
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import munit.FunSuite

final class ClusterSnapshotParticipantSuite extends FunSuite:
  private val snapshotId = UUID.fromString("12345678-1234-5678-9234-567812345678")
  private val identity = ClusterSnapshotIdentity(snapshotId, 1, 4L, 9L, "a" * 64)

  test("prepares, captures and releases a fenced node artifact") {
    val root = Files.createTempDirectory("cascade-snapshot-participant")
    val barrier = SnapshotRequestBarrier()
    val participant = ClusterSnapshotParticipant(1, () => position(), barrier, _ => content)
    try
      participant.prepare(SnapshotPrepareRequest(identity, 5000L))
      val artifact = participant.capture(SnapshotCaptureRequest(identity, root.resolve("node-1").toString))
      assertEquals(artifact.nodeId, 1)
      assertEquals(artifact.directoryId, "11111111-1111-4111-8111-111111111111")
      assertEquals(artifact.watermarks, content.watermarks)
      assert(barrier.isOwnedBy(snapshotId))
      participant.release(snapshotId)
      participant.release(snapshotId)
      assert(!barrier.isOwnedBy(snapshotId))
    finally Files.deleteIfExists(root): Unit
  }

  test("rejects stale positions, joint membership, unavailable voters and wrong target binding") {
    val barrier = SnapshotRequestBarrier()
    val stale = ClusterSnapshotParticipant(1, () => position().copy(metadataVersion = 10L), barrier, _ => content)
    intercept[IllegalArgumentException](stale.prepare(SnapshotPrepareRequest(identity, 1000L))): Unit
    val joint = position().copy(membership = position().membership.beginTransition(position().membership.currentVoters.dropRight(1)))
    intercept[IllegalArgumentException](
      ClusterSnapshotParticipant(1, () => joint, barrier, _ => content).prepare(SnapshotPrepareRequest(identity, 1000L))
    ): Unit
    val unavailable = ClusterSnapshotParticipant(
      1, () => position().copy(unavailableBrokerIds = Set(2)), barrier, _ => content
    )
    intercept[IllegalArgumentException](unavailable.prepare(SnapshotPrepareRequest(identity, 1000L))): Unit

    val valid = ClusterSnapshotParticipant(1, () => position(), barrier, _ => content)
    valid.prepare(SnapshotPrepareRequest(identity, 1000L))
    try
      intercept[IllegalArgumentException](
        valid.capture(SnapshotCaptureRequest(identity, Files.createTempDirectory("wrong-node").resolve("node-2").toString))
      ): Unit
    finally valid.release(snapshotId)
  }

  test("rejects an artifact when metadata changes during its local capture") {
    val current = AtomicReference(position())
    val barrier = SnapshotRequestBarrier()
    val participant = ClusterSnapshotParticipant(1, () => current.get(), barrier, _ => {
      current.set(current.get().copy(metadataVersion = 10L))
      content
    })
    participant.prepare(SnapshotPrepareRequest(identity, 1000L))
    try
      intercept[IllegalArgumentException](
        participant.capture(SnapshotCaptureRequest(identity, Files.createTempDirectory("snapshot-change").resolve("node-1").toString))
      ): Unit
      assert(barrier.isOwnedBy(snapshotId))
    finally participant.release(snapshotId)
  }

  private def content: LocalSnapshotContent = LocalSnapshotContent(
    "b" * 64,
    4,
    1024L,
    Vector(SnapshotWatermark("orders", 0, 10L))
  )

  private def position(): ClusterSnapshotPosition =
    val voters = Vector(1, 2, 3).map { id =>
      val directory = VoterDirectoryId(
        UUID.fromString(s"$id$id$id$id$id$id$id$id-$id$id$id$id-4$id$id$id-8$id$id$id-$id$id$id$id$id$id$id$id$id$id$id$id").getMostSignificantBits,
        UUID.fromString(s"$id$id$id$id$id$id$id$id-$id$id$id$id-4$id$id$id-8$id$id$id-$id$id$id$id$id$id$id$id$id$id$id$id").getLeastSignificantBits
      )
      QuorumVoter(ClusterNode(id, "127.0.0.1", 9000 + id), directory)
    }
    ClusterSnapshotPosition(1, 4L, 9L, "a" * 64, QuorumMembership(voters), Set.empty, Vector.empty)

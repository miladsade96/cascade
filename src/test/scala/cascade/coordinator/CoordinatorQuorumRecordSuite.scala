package cascade.coordinator

import cascade.cluster.{ClusterNode, QuorumMembership}
import munit.FunSuite

final class CoordinatorQuorumRecordSuite extends FunSuite:
  private val transaction = CoordinatorTransactionId(1L, 2L)
  private val delta = CoordinatorDelta(3L, Vector(
    CoordinatorShardUpdate(7, 0L, Vector(1.toByte)),
    CoordinatorShardUpdate(2, 4L, Vector(2.toByte))
  ))
  private val certificate = CoordinatorDecisionCertificate.from(
    QuorumMembership.bootstrap(Vector(ClusterNode(1, "one", 1), ClusterNode(2, "two", 2), ClusterNode(3, "three", 3))),
    Set(1, 2)
  )

  test("prepare records expose a sorted distinct shard set") {
    val record = CoordinatorQuorumRecord.prepare(transaction, delta)
    assertEquals(record.phase, CoordinatorQuorumPhase.Prepare)
    assertEquals(record.shards, Vector(2, 7))
    assertEquals(record.delta, Some(delta))
  }

  test("decision markers never carry mutable coordinator state") {
    Vector(CoordinatorQuorumPhase.Decide, CoordinatorQuorumPhase.Finalize, CoordinatorQuorumPhase.Abort).foreach { phase =>
      val record = CoordinatorQuorumRecord.marker(transaction, phase)
      assertEquals(record.delta, None)
      assertEquals(record.shards, Vector.empty)
    }
    intercept[IllegalArgumentException](CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Prepare))
    intercept[IllegalArgumentException](CoordinatorQuorumRecord(transaction, CoordinatorQuorumPhase.Decide, Some(delta)))
  }

  test("commit and recovery records carry a verifiable decision certificate") {
    val commit = CoordinatorQuorumRecord.commit(transaction, certificate)
    assertEquals(commit.delta, None)
    assertEquals(commit.certificate, Some(certificate))
    val recovery = CoordinatorQuorumRecord.recover(transaction, delta, certificate)
    assertEquals(recovery.shards, Vector(2, 7))
    assertEquals(recovery.certificate, Some(certificate))
    intercept[IllegalArgumentException](CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Commit))
    intercept[IllegalArgumentException](CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Recover))
  }

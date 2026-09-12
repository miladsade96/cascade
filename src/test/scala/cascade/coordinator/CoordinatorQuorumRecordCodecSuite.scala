package cascade.coordinator

import cascade.cluster.{ClusterNode, QuorumMembership}
import cascade.protocol.ProtocolException
import munit.FunSuite

final class CoordinatorQuorumRecordCodecSuite extends FunSuite:
  private val transaction = CoordinatorTransactionId(Long.MinValue, Long.MaxValue)
  private val delta = CoordinatorDelta(19L, Vector(
    CoordinatorShardUpdate(0, 4L, Vector(0, 1, -1).map(_.toByte)),
    CoordinatorShardUpdate(CoordinatorShard.Allocator, 8L, Vector.fill(16)(9.toByte))
  ))
  private val certificate = CoordinatorDecisionCertificate.from(
    QuorumMembership.bootstrap(Vector(ClusterNode(1, "one", 1), ClusterNode(2, "two", 2), ClusterNode(3, "three", 3))),
    Set(1, 3)
  )

  test("round trips prepare and marker records") {
    val records = Vector(
      CoordinatorQuorumRecord.prepare(transaction, delta),
      CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Decide),
      CoordinatorQuorumRecord.commit(transaction, certificate),
      CoordinatorQuorumRecord.recover(transaction, delta, certificate),
      CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Finalize),
      CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Abort)
    )
    records.foreach(record => assertEquals(CoordinatorQuorumRecordCodec.decode(CoordinatorQuorumRecordCodec.encode(record)), record))
  }

  test("rejects malformed, trailing, and oversized records") {
    val valid = CoordinatorQuorumRecordCodec.encode(CoordinatorQuorumRecord.prepare(transaction, delta))
    intercept[ProtocolException](CoordinatorQuorumRecordCodec.decode(valid.updated(1, 99.toByte)))
    intercept[ProtocolException](CoordinatorQuorumRecordCodec.decode(valid.updated(2, 99.toByte)))
    intercept[ProtocolException](CoordinatorQuorumRecordCodec.decode(valid ++ Array(0.toByte)))
    intercept[ProtocolException](CoordinatorQuorumRecordCodec.decode(Array.ofDim[Byte](CoordinatorQuorumRecordCodec.MaximumBytes + 1)))
  }

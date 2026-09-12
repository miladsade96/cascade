package cascade.coordinator

import cascade.cluster.InternalApi
import cascade.protocol.{ByteCursor, ProtocolException}
import munit.FunSuite

final class CoordinatorQuorumRequestCodecSuite extends FunSuite:
  test("round trips bounded quorum requests and reserves internal API keys") {
    val transaction = CoordinatorTransactionId(5L, 6L)
    val delta = CoordinatorDelta(7L, Vector(CoordinatorShardUpdate(4, 3L, Vector(1.toByte))))
    val request = CoordinatorQuorumRequest(7L, CoordinatorQuorumRecord.prepare(transaction, delta))
    assertEquals(CoordinatorQuorumRequestCodec.decode(ByteCursor(CoordinatorQuorumRequestCodec.encode(request))), request)
    assert(InternalApi.contains(InternalApi.CoordinatorShardPrepare))
    assert(InternalApi.contains(InternalApi.CoordinatorShardDecide))
    assert(InternalApi.contains(InternalApi.CoordinatorShardFinalize))
    assert(InternalApi.contains(InternalApi.CoordinatorShardAbort))
    assert(InternalApi.contains(InternalApi.CoordinatorShardCommit))
    assert(InternalApi.contains(InternalApi.CoordinatorShardRecover))
    assert(InternalApi.contains(InternalApi.CoordinatorDecisionQuery))
    assert(!InternalApi.contains((InternalApi.CoordinatorDecisionQuery - 1).toShort))
  }

  test("rejects trailing and negative-term requests") {
    val marker = CoordinatorQuorumRecord.marker(CoordinatorTransactionId(1L, 1L), CoordinatorQuorumPhase.Decide)
    val valid = CoordinatorQuorumRequestCodec.encode(CoordinatorQuorumRequest(1L, marker))
    intercept[ProtocolException](CoordinatorQuorumRequestCodec.decode(ByteCursor(valid ++ Array(0.toByte))))
    intercept[IllegalArgumentException](CoordinatorQuorumRequest(-1L, marker))
  }

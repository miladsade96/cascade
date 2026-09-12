package cascade.coordinator

import cascade.cluster.{ClusterNode, QuorumMembership}
import cascade.protocol.{ByteCursor, Errors, ProtocolException}
import munit.FunSuite

final class CoordinatorDecisionQueryCodecSuite extends FunSuite:
  private val transaction = CoordinatorTransactionId(11L, 29L)
  private val certificate = CoordinatorDecisionCertificate.from(
    QuorumMembership.bootstrap(Vector(ClusterNode(1, "one", 1), ClusterNode(2, "two", 2), ClusterNode(3, "three", 3))),
    Set(1, 2)
  )

  test("round trips decision queries and certified responses") {
    val query = CoordinatorDecisionQuery(transaction)
    assertEquals(CoordinatorDecisionQueryCodec.decode(ByteCursor(CoordinatorDecisionQueryCodec.encode(query))), query)
    val result = CoordinatorDecisionQueryResult(Errors.None, CoordinatorTransactionStatus.Committed, Some(certificate))
    assertEquals(CoordinatorDecisionQueryCodec.decodeResult(ByteCursor(CoordinatorDecisionQueryCodec.encodeResult(result))), result)
  }

  test("rejects trailing query and response bytes") {
    intercept[ProtocolException](CoordinatorDecisionQueryCodec.decode(
      ByteCursor(CoordinatorDecisionQueryCodec.encode(CoordinatorDecisionQuery(transaction)) ++ Array(0.toByte))
    ))
    val result = CoordinatorDecisionQueryResult(Errors.None, CoordinatorTransactionStatus.Unknown, None)
    intercept[ProtocolException](CoordinatorDecisionQueryCodec.decodeResult(
      ByteCursor(CoordinatorDecisionQueryCodec.encodeResult(result) ++ Array(0.toByte))
    ))
  }

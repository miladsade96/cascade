package cascade.coordinator

import cascade.cluster.{ClusterNode, QuorumMembership}
import cascade.protocol.{ByteCursor, ProtocolException}
import munit.FunSuite

final class CoordinatorDecisionCertificateSuite extends FunSuite:
  private val membership = QuorumMembership.bootstrap(Vector(
    ClusterNode(1, "one", 9092),
    ClusterNode(2, "two", 9092),
    ClusterNode(3, "three", 9092)
  ))

  test("a stable decision certificate requires a majority") {
    val certificate = CoordinatorDecisionCertificate.from(membership, Set(1, 3))
    assert(certificate.valid)
    assertEquals(certificate.acknowledgedNodeIds, Vector(1, 3))
    intercept[IllegalArgumentException](CoordinatorDecisionCertificate.from(membership, Set(1)))
  }

  test("a joint decision certificate proves both voter majorities") {
    val target = QuorumMembership.bootstrap(Vector(
      ClusterNode(2, "two", 9092),
      ClusterNode(3, "three", 9092),
      ClusterNode(4, "four", 9092)
    )).currentVoters
    val joint = membership.beginTransition(target)
    assert(CoordinatorDecisionCertificate.from(joint, Set(1, 2, 4)).valid)
    intercept[IllegalArgumentException](CoordinatorDecisionCertificate.from(joint, Set(1, 2)))
  }

  test("certificate codec retains voter directory identities") {
    val certificate = CoordinatorDecisionCertificate.from(membership, Set(1, 2))
    val writer = cascade.protocol.ByteWriter()
    CoordinatorDecisionCertificateCodec.write(writer, certificate)
    assertEquals(CoordinatorDecisionCertificateCodec.read(ByteCursor(writer.result())), certificate)
  }

  test("certificate codec rejects a forged minority") {
    val certificate = CoordinatorDecisionCertificate.from(membership, Set(1, 2))
    val writer = cascade.protocol.ByteWriter()
    CoordinatorDecisionCertificateCodec.write(writer, certificate)
    val bytes = writer.result()
    // The final array contains two acknowledgements; advertise only one while retaining trailing bytes.
    val acknowledgementCountOffset = bytes.length - (Integer.BYTES * 3)
    val malformed = bytes.updated(acknowledgementCountOffset + 3, 1.toByte)
    val cursor = ByteCursor(malformed)
    CoordinatorDecisionCertificateCodec.read(cursor)
    intercept[ProtocolException](cursor.ensureFullyRead())
  }

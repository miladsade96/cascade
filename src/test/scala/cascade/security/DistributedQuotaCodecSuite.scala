package cascade.security

import cascade.protocol.{ByteCursor, Errors, ProtocolException}
import munit.FunSuite

final class DistributedQuotaCodecSuite extends FunSuite:
  test("round trips cluster quota reservations") {
    QuotaKind.All.foreach { kind =>
      val expected = ClusterQuotaReservation(kind, "User:tenant-a", 4096, rejectExcess = true, QuotaLimit(8192, 16384, 750), 42)
      val actual = DistributedQuotaCodec.decodeReservation(ByteCursor(DistributedQuotaCodec.encodeReservation(expected)))
      assertEquals(actual, expected)
    }
  }

  test("round trips every quota result") {
    val results = Vector(
      ClusterQuotaResult(Errors.NotController, None, 8),
      ClusterQuotaResult(Errors.None, Some(QuotaDecision.Allowed), 9),
      ClusterQuotaResult(Errors.None, Some(QuotaDecision.Throttle(27)), 10),
      ClusterQuotaResult(Errors.None, Some(QuotaDecision.Rejected(81)), 11)
    )
    results.foreach { expected =>
      val actual = DistributedQuotaCodec.decodeResult(ByteCursor(DistributedQuotaCodec.encodeResult(expected)))
      assertEquals(actual, expected)
    }
  }

  test("rejects unknown quota kinds and invalid decisions") {
    val reservation = DistributedQuotaCodec.encodeReservation(
      ClusterQuotaReservation(QuotaKind.Request, "tenant", 1, rejectExcess = true, QuotaLimit(1, 1, 1), 1)
    )
    reservation(0) = 99.toByte
    intercept[ProtocolException](DistributedQuotaCodec.decodeReservation(ByteCursor(reservation)))

    val result = DistributedQuotaCodec.encodeResult(ClusterQuotaResult(Errors.None, Some(QuotaDecision.Allowed), 1))
    result(result.length - java.lang.Long.BYTES - 1) = 99.toByte
    intercept[ProtocolException](DistributedQuotaCodec.decodeResult(ByteCursor(result)))
  }

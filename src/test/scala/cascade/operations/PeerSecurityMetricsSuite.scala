package cascade.operations

import munit.FunSuite

final class PeerSecurityMetricsSuite extends FunSuite:
  test("counts authenticated, encrypted, and rejected peer requests independently") {
    val metrics = PeerSecurityMetrics()

    metrics.recordAuthenticated(encrypted = false)
    metrics.recordAuthenticated(encrypted = true)
    metrics.recordRejected()
    metrics.recordRejected()

    assertEquals(metrics.snapshot, PeerSecuritySnapshot(2L, 1L, 2L))
  }

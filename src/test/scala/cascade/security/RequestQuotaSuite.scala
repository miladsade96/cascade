package cascade.security

import munit.FunSuite

final class RequestQuotaSuite extends FunSuite:
  test("isolates token buckets by principal and rejects excessive delay") {
    var now = 0L
    val quota = RequestQuota(
      bytesPerSecond = 1000L,
      configuredBurstBytes = 1000L,
      maxThrottleMillis = 500L,
      nanoTime = () => now
    )

    assertEquals(quota.evaluate("alice", 1000), QuotaDecision.Allowed)
    assertEquals(quota.evaluate("alice", 250), QuotaDecision.Throttle(250L))
    assertEquals(quota.evaluate("bob", 1000), QuotaDecision.Allowed)
    assertEquals(quota.evaluate("alice", 1000), QuotaDecision.Rejected(1250L))

    now += 250_000_000L
    assertEquals(quota.evaluate("alice", 1), QuotaDecision.Throttle(1L))
    assertEquals(quota.snapshot.throttled, 2L)
    assertEquals(quota.snapshot.rejected, 1L)
    assertEquals(quota.snapshot.principals, 2)
  }

  test("disables quota work when the configured rate is zero") {
    val quota = RequestQuota(0L, 0L, 0L)
    assertEquals(quota.evaluate("alice", Int.MaxValue), QuotaDecision.Allowed)
    assertEquals(quota.snapshot.principals, 0)
  }

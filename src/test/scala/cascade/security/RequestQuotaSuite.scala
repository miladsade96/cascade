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

  test("fully delays egress instead of leaking acknowledged bytes above the limit") {
    val quota = RequestQuota(1000L, 1L, 25L, () => 0L)
    assertEquals(quota.evaluate("alice", 1000, rejectExcess = false), QuotaDecision.Throttle(999L))
    assertEquals(quota.snapshot.throttled, 1L)
    assertEquals(quota.snapshot.rejected, 0L)
    assertEquals(quota.snapshot.throttleMillis, 999L)
  }

  test("uses a distributed decision before the conservative local share") {
    var delegated = 0
    val quota = RequestQuota(
      1000L,
      1000L,
      1000L,
      () => 0L,
      () => 3,
      (principal, bytes, rejectExcess) =>
        delegated += 1
        assertEquals(principal, "tenant")
        assertEquals(bytes, 600)
        assert(rejectExcess)
        Some(QuotaDecision.Allowed)
    )

    assertEquals(quota.evaluate("tenant", 600), QuotaDecision.Allowed)
    assertEquals(delegated, 1)
    assertEquals(quota.snapshot.principals, 1)
  }

  test("cluster shares bound aggregate tenant bursts across brokers") {
    val first = RequestQuota(2000L, 2000L, 1000L, () => 0L, () => 2)
    val second = RequestQuota(2000L, 2000L, 1000L, () => 0L, () => 2)

    assertEquals(first.evaluate("tenant-a", 1000), QuotaDecision.Allowed)
    assertEquals(second.evaluate("tenant-a", 1000), QuotaDecision.Allowed)
    assert(first.evaluate("tenant-a", 1).isInstanceOf[QuotaDecision.Throttle])
    assert(second.evaluate("tenant-a", 1).isInstanceOf[QuotaDecision.Throttle])
    assertEquals(first.evaluate("tenant-b", 1000), QuotaDecision.Allowed)
    assertEquals(second.evaluate("tenant-b", 1000), QuotaDecision.Allowed)
  }

  test("fractional cluster shares never round a small aggregate quota upward") {
    val brokers = Vector.fill(3)(RequestQuota(1L, 1L, 5000L, () => 0L, () => 3))
    assert(brokers.forall(_.evaluate("tenant", 1).isInstanceOf[QuotaDecision.Throttle]))
  }

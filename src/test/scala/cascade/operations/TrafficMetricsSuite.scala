package cascade.operations

import munit.FunSuite

final class TrafficMetricsSuite extends FunSuite:
  test("records request traffic with monotonic counters") {
    val metrics = TrafficMetrics()
    metrics.recordRequest(100)
    metrics.recordRequest(200)
    metrics.recordResponse(75)
    metrics.recordFailure()
    metrics.recordDuration(500L)
    metrics.recordDuration(-1L)

    assertEquals(metrics.snapshot, TrafficSnapshot(2L, 300L, 1L, 75L, 1L, 500L))
  }

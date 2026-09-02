package cascade.coordinator

import munit.FunSuite

final class CoordinatorMetricsSuite extends FunSuite:
  test("metrics count successes, rejections, bytes, and nonnegative durations without key labels") {
    val metrics = CoordinatorMetrics()
    metrics.record(true, 100L, 10000L, 1, 25L)
    metrics.record(false, 200L, 12000L, 2, -1L)
    assertEquals(metrics.snapshot, CoordinatorMetricsSnapshot(2L, 1L, 300L, 22000L, 3L, 25L))
  }

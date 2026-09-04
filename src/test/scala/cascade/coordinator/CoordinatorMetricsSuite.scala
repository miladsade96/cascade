package cascade.coordinator

import munit.FunSuite

final class CoordinatorMetricsSuite extends FunSuite:
  test("metrics count successes, rejections, bytes, and nonnegative durations without key labels") {
    val metrics = CoordinatorMetrics()
    metrics.record(true, 100L, 10000L, 1, 25L)
    metrics.record(false, 200L, 12000L, 2, -1L)
    assertEquals(metrics.snapshot, CoordinatorMetricsSnapshot(2L, 1L, 300L, 22000L, 3L, 25L))
  }

  test("preparation counters measure local encoding and reuse independently from publication success") {
    val metrics = CoordinatorMetrics()
    metrics.recordPreparation(CoordinatorSnapshot(Vector.empty, 10000L, 2, 127, 123L), 456L)
    metrics.recordPreparation(CoordinatorSnapshot(Vector.empty, 10000L, 0, 129, 0L), -1L)
    metrics.record(false, 123L, 10000L, 2, 1000L)
    assertEquals(metrics.snapshot.encodedShards, 2L)
    assertEquals(metrics.snapshot.reusedShards, 256L)
    assertEquals(metrics.snapshot.encodedBytes, 123L)
    assertEquals(metrics.snapshot.preparationNanos, 456L)
    assertEquals(metrics.snapshot.failures, 1L)
  }

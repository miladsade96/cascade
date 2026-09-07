package cascade.coordinator

import munit.FunSuite

final class CoordinatorReadMetricsSuite extends FunSuite:
  test("records acknowledged view activity without key labels") {
    val metrics = CoordinatorReadMetrics()
    metrics.recordOffsets(3)
    metrics.recordOffsets(0)
    metrics.recordStableOffset()
    metrics.recordStableOffset()
    metrics.recordTransactionVisibility()

    assertEquals(
      metrics.snapshot,
      CoordinatorReadSnapshot(
        offsetSnapshots = 2L,
        offsetKeys = 3L,
        stableOffsetSnapshots = 2L,
        transactionVisibilitySnapshots = 1L
      )
    )
  }

  test("clamps invalid key counts without losing the request") {
    val metrics = CoordinatorReadMetrics()
    metrics.recordOffsets(-1)

    assertEquals(metrics.snapshot.offsetSnapshots, 1L)
    assertEquals(metrics.snapshot.offsetKeys, 0L)
  }

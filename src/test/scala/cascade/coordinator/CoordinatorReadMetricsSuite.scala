package cascade.coordinator

import java.util.concurrent.{Executors, TimeUnit}
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

  test("records group administration without group ID labels") {
    val metrics = CoordinatorReadMetrics()
    metrics.recordGroupList(3)
    metrics.recordGroupList(-1)
    metrics.recordGroupDescribe(found = true)
    metrics.recordGroupDescribe(found = false)
    metrics.recordGroupDelete(succeeded = true)
    metrics.recordGroupDelete(succeeded = false)

    val snapshot = metrics.snapshot
    assertEquals(snapshot.groupListSnapshots, 2L)
    assertEquals(snapshot.groupListEntries, 3L)
    assertEquals(snapshot.groupDescribeSnapshots, 2L)
    assertEquals(snapshot.groupDescribeHits, 1L)
    assertEquals(snapshot.groupDeleteAttempts, 2L)
    assertEquals(snapshot.groupDeleteFailures, 1L)
  }

  test("retains exact totals under concurrent read traffic") {
    val metrics = CoordinatorReadMetrics()
    val workers = 8
    val iterations = 10000
    val executor = Executors.newFixedThreadPool(workers)
    try
      val futures = (0 until workers).map { _ =>
        executor.submit(new Runnable:
          override def run(): Unit =
            (0 until iterations).foreach { _ =>
              metrics.recordOffsets(2)
              metrics.recordStableOffset()
              metrics.recordTransactionVisibility()
            }
        )
      }
      futures.foreach(_.get(10L, TimeUnit.SECONDS))

      val total = workers.toLong * iterations
      assertEquals(metrics.snapshot, CoordinatorReadSnapshot(total, total * 2L, total, total))
    finally
      executor.shutdownNow(): Unit
      executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
  }

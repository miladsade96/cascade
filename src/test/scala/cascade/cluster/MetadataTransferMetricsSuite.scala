package cascade.cluster

import munit.FunSuite

final class MetadataTransferMetricsSuite extends FunSuite:
  test("parallel peer attempts accumulate exact byte and fallback counters") {
    val metrics = MetadataTransferMetrics()
    val threads = Vector.fill(4)(Thread.ofVirtual().start(() => (1 to 1000).foreach { _ =>
      metrics.recordDelta(10)
      metrics.recordFull(100)
      metrics.recordFallback()
    }))
    threads.foreach(_.join())
    assertEquals(metrics.snapshot, MetadataTransferSnapshot(40000L, 400000L, 4000L))
  }

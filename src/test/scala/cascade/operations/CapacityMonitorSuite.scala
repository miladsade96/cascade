package cascade.operations

import scala.collection.mutable.ArrayBuffer

final class CapacityMonitorSuite extends munit.FunSuite:
  test("deduplicates active alerts, repeats them on schedule, and emits resolution") {
    var now = 1000L
    var current = snapshot(activeConnections = 9)
    val raised = ArrayBuffer.empty[String]
    val resolved = ArrayBuffer.empty[String]
    val monitor = CapacityMonitor(
      CapacityAlertConfig(
        intervalMillis = 1000L,
        connectionUtilization = 0.9d,
        repeatIntervalMillis = 5000L,
        pendingFlushBytes = 0L
      ),
      () => current,
      CapacityLimits(10, 10),
      alert => raised += alert.code,
      code => resolved += code,
      clockMillis = () => now
    )
    try
      monitor.poll()
      monitor.poll()
      assertEquals(raised.toVector, Vector("connections_near_limit"))
      assertEquals(monitor.activeAlerts, Set("connections_near_limit"))

      now += 5000L
      monitor.poll()
      assertEquals(raised.toVector, Vector("connections_near_limit", "connections_near_limit"))

      current = snapshot(activeConnections = 0)
      monitor.poll()
      assertEquals(resolved.toVector, Vector("connections_near_limit"))
      assertEquals(monitor.activeAlerts, Set.empty)
    finally monitor.close()
  }

  private def snapshot(activeConnections: Int): BrokerMetricsSnapshot =
    BrokerMetricsSnapshot(
      1, 1L, true, false, 1, false, 0, 0, activeConnections, 0L, 0, 0L, 0, 0L, 0L, 0L,
      TrafficSnapshot(0L, 0L, 0L, 0L, 0L, 0L), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
      1024L, 2048L, 0L, 1024L
    )

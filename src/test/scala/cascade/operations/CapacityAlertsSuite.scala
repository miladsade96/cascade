package cascade.operations

final class CapacityAlertsSuite extends munit.FunSuite:
  test("emits deterministic alerts at configured capacity boundaries") {
    val alerts = CapacityAlerts.evaluate(
      snapshot(activeConnections = 85, activeRequests = 9, pendingFlushBytes = 512L, usableDiskBytes = 99L),
      CapacityLimits(100, 10),
      CapacityAlertConfig(
        connectionUtilization = 0.85d,
        inFlightUtilization = 0.85d,
        pendingFlushBytes = 512L,
        minimumFreeBytes = 100L
      )
    )

    assertEquals(
      alerts.map(_.code),
      Vector("connections_near_limit", "requests_near_limit", "flush_backlog_high", "disk_space_low")
    )
    assertEquals(alerts.head.threshold, 85L)
    assertEquals(alerts(1).threshold, 9L)
  }

  test("does not alert below boundaries and allows optional byte alerts to be disabled") {
    val alerts = CapacityAlerts.evaluate(
      snapshot(activeConnections = 8, activeRequests = 3, pendingFlushBytes = Long.MaxValue, usableDiskBytes = 0L),
      CapacityLimits(10, 4),
      CapacityAlertConfig(connectionUtilization = 0.9d, inFlightUtilization = 1d, pendingFlushBytes = 0L, minimumFreeBytes = 0L)
    )

    assertEquals(alerts, Vector.empty)
  }

  private def snapshot(
      activeConnections: Int,
      activeRequests: Int,
      pendingFlushBytes: Long,
      usableDiskBytes: Long
  ): BrokerMetricsSnapshot =
    BrokerMetricsSnapshot(
      1, 1L, true, false, 1, false, 0, 0, activeConnections, 0L, activeRequests, 0L, 0, 0L, 0L, 0L,
      TrafficSnapshot(0L, 0L, 0L, 0L, 0L, 0L), 0L, 0L, 0L, pendingFlushBytes, 0L, 0L, 0L, 0L,
      usableDiskBytes, 1024L, 0L, 1024L
    )

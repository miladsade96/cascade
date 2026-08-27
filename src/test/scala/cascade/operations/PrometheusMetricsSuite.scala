package cascade.operations

import munit.FunSuite

final class PrometheusMetricsSuite extends FunSuite:
  test("encodes deterministic Prometheus 0.0.4 counters and gauges") {
    val output = PrometheusMetrics.encode(snapshot)
    assert(output.endsWith("\n"))
    assert(output.contains("# TYPE cascade_broker_up gauge\ncascade_broker_up{node_id=\"7\"} 1.0\n"))
    assert(output.contains("# TYPE cascade_requests_total counter\ncascade_requests_total{node_id=\"7\"} 11.0\n"))
    assert(output.contains("cascade_request_processing_seconds_total{node_id=\"7\"} 1.5\n"))
    assert(output.contains("cascade_peer_tls_authentications_total{node_id=\"7\"} 29.0\n"))
    assert(output.contains("cascade_peer_authentication_rejections_total{node_id=\"7\"} 30.0\n"))
    assert(output.contains("cascade_sasl_authentication_successes_total{mechanism=\"SCRAM-SHA-256\",node_id=\"7\"} 2.0\n"))
    assert(output.contains("cascade_sasl_authentication_failures_total{mechanism=\"UNKNOWN\",node_id=\"7\"} 7.0\n"))
    assertEquals(output.linesIterator.count(_.startsWith("cascade_")), 44)
  }

  private val snapshot = BrokerMetricsSnapshot(
    nodeId = 7,
    uptimeMillis = 2500L,
    running = true,
    clustered = true,
    controllerId = 3,
    brokerFenced = false,
    topics = 2,
    partitions = 4,
    activeConnections = 5,
    rejectedConnections = 1L,
    activeRequests = 2,
    rejectedRequests = 3L,
    quotaPrincipals = 2,
    quotaThrottledRequests = 4L,
    quotaRejectedRequests = 5L,
    quotaThrottleMillis = 600L,
    traffic = TrafficSnapshot(11L, 12L, 13L, 14L, 15L, 1_500_000_000L),
    flushOperations = 16L,
    flushBytes = 17L,
    flushNanos = 18L,
    pendingFlushBytes = 19L,
    lifecycleRuns = 20L,
    retiredSegments = 21L,
    reclaimedBytes = 22L,
    rejectedAppends = 23L,
    usableDiskBytes = 24L,
    totalDiskBytes = 25L,
    heapUsedBytes = 26L,
    heapMaxBytes = 27L,
    peerSecurity = PeerSecuritySnapshot(28L, 29L, 30L),
    authentication = AuthenticationSnapshot(
      Vector(
        MechanismAuthenticationSnapshot("PLAIN", 1L, 4L),
        MechanismAuthenticationSnapshot("SCRAM-SHA-256", 2L, 5L),
        MechanismAuthenticationSnapshot("SCRAM-SHA-512", 3L, 6L),
        MechanismAuthenticationSnapshot("UNKNOWN", 0L, 7L)
      )
    )
  )

package cascade.operations

class BrokerHealthSuite extends munit.FunSuite:
  test("reports a running broker with capacity as live and ready") {
    val health = BrokerHealth.evaluate(snapshot(), HealthPolicy(1024L, 512L), None)

    assert(health.live)
    assert(health.ready)
    assertEquals(health.failedChecks, Vector.empty)
  }

  test("separates process liveness from traffic readiness") {
    val health = BrokerHealth.evaluate(
      snapshot(brokerFenced = true, pendingFlushBytes = 2048L, usableDiskBytes = 100L),
      HealthPolicy(1024L, 512L),
      Some("disk full")
    )

    assert(health.live)
    assert(!health.ready)
    assertEquals(health.failedChecks.map(_.name), Vector("broker_unfenced", "flush_backlog", "disk_reserve", "structured_log"))
  }

  test("marks a stopped broker as neither live nor ready") {
    val health = BrokerHealth.evaluate(snapshot(running = false), HealthPolicy(Long.MaxValue, 0L), None)

    assert(!health.live)
    assert(!health.ready)
    assertEquals(health.failedChecks.map(_.name), Vector("broker_running"))
  }

  test("keeps liveness but fails readiness when peer identity policy reload fails") {
    val health = BrokerHealth.evaluate(
      snapshot(),
      HealthPolicy(Long.MaxValue, 0L),
      structuredLogFailure = None,
      peerIdentityFailure = Some("invalid peer identity entry")
    )

    assert(health.live)
    assert(!health.ready)
    assertEquals(health.failedChecks.map(_.name), Vector("peer_identity_policy"))
    assertEquals(health.failedChecks.head.detail, "invalid peer identity entry")
  }

  test("fails readiness without failing liveness when a credential reload is malformed") {
    val health = BrokerHealth.evaluate(
      snapshot(),
      HealthPolicy(Long.MaxValue, 0L),
      structuredLogFailure = None,
      peerIdentityFailure = None,
      credentialFailure = Some("invalid SCRAM credential")
    )

    assert(health.live)
    assert(!health.ready)
    assertEquals(health.failedChecks.map(_.name), Vector("credential_policy"))
  }

  test("keeps serving the last valid context while a bad TLS rotation fails readiness") {
    val health = BrokerHealth.evaluate(
      snapshot(),
      HealthPolicy(Long.MaxValue, 0L),
      structuredLogFailure = None,
      tlsMaterialFailure = Some("invalid replacement key store")
    )

    assert(health.live)
    assert(!health.ready)
    assertEquals(health.failedChecks.map(_.name), Vector("tls_material"))
    assertEquals(health.failedChecks.head.detail, "invalid replacement key store")
  }

  private def snapshot(
      running: Boolean = true,
      brokerFenced: Boolean = false,
      pendingFlushBytes: Long = 0L,
      usableDiskBytes: Long = 1024L
  ): BrokerMetricsSnapshot =
    BrokerMetricsSnapshot(
      nodeId = 1,
      uptimeMillis = 1L,
      running = running,
      clustered = false,
      controllerId = 1,
      brokerFenced = brokerFenced,
      topics = 0,
      partitions = 0,
      activeConnections = 0,
      rejectedConnections = 0L,
      activeRequests = 0,
      rejectedRequests = 0L,
      quotaPrincipals = 0,
      quotaThrottledRequests = 0L,
      quotaRejectedRequests = 0L,
      quotaThrottleMillis = 0L,
      traffic = TrafficSnapshot(0L, 0L, 0L, 0L, 0L, 0L),
      flushOperations = 0L,
      flushBytes = 0L,
      flushNanos = 0L,
      pendingFlushBytes = pendingFlushBytes,
      lifecycleRuns = 0L,
      retiredSegments = 0L,
      reclaimedBytes = 0L,
      rejectedAppends = 0L,
      usableDiskBytes = usableDiskBytes,
      totalDiskBytes = 2048L,
      heapUsedBytes = 0L,
      heapMaxBytes = 1024L
    )

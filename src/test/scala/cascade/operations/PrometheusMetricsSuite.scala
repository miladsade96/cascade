package cascade.operations

import cascade.security.{RequestQuotaSnapshot, TlsReloadSnapshot}
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
    assert(output.contains("cascade_tls_enabled{node_id=\"7\"} 1.0\n"))
    assert(output.contains("cascade_tls_material_generation{node_id=\"7\"} 4.0\n"))
    assert(output.contains("cascade_tls_material_reloads_total{node_id=\"7\"} 3.0\n"))
    assert(output.contains("cascade_tls_material_reload_failures_total{node_id=\"7\"} 2.0\n"))
    assert(output.contains("cascade_sasl_authentication_successes_total{mechanism=\"SCRAM-SHA-256\",node_id=\"7\"} 2.0\n"))
    assert(output.contains("cascade_sasl_authentication_successes_total{mechanism=\"OAUTHBEARER\",node_id=\"7\"} 8.0\n"))
    assert(output.contains("cascade_sasl_authentication_failures_total{mechanism=\"UNKNOWN\",node_id=\"7\"} 7.0\n"))
    assert(output.contains("cascade_traffic_quota_throttled_total{node_id=\"7\",quota=\"fetch\"} 19.0\n"))
    assert(output.contains("cascade_coordinator_delta_bytes_total{node_id=\"7\"} 0.0\n"))
    assertEquals(output.linesIterator.count(_.startsWith("cascade_")), 103)
  }

  test("metadata persistence and replication expose bounded node-only measurements") {
    val measured = snapshot.copy(
      metadataJournal = cascade.cluster.MetadataJournalSnapshot(2L, 3L, 40L, 50L, 60L, 70L),
      shardObjects = cascade.cluster.ShardObjectSnapshot(writtenBytes = 100L, liveBytes = 90L, directoryForceSupported = true),
      metadataTransfers = cascade.cluster.MetadataTransferSnapshot(80L, 90L, 1L))
    val output = PrometheusMetrics.encode(measured)
    assert(output.contains("cascade_metadata_journal_delta_bytes_total{node_id=\"7\"} 50.0\n"))
    assert(output.contains("cascade_metadata_checkpoint_bytes_total{node_id=\"7\"} 60.0\n"))
    assert(output.contains("cascade_metadata_replication_full_bytes_total{node_id=\"7\"} 90.0\n"))
    assert(output.contains("cascade_metadata_replication_fallbacks_total{node_id=\"7\"} 1.0\n"))
    assert(!output.contains("group_id="))
    assert(output.contains("cascade_coordinator_object_bytes_written_total{node_id=\"7\"} 100.0\n"))
    assert(output.contains("cascade_coordinator_object_bytes{node_id=\"7\"} 90.0\n"))
    assert(output.contains("cascade_coordinator_directory_force_supported{node_id=\"7\"} 1.0\n"))
  }

  test("offset batching metrics expose bounds and outcomes without tenant labels") {
    val output = PrometheusMetrics.encode(snapshot.copy(offsetBatch = cascade.group.OffsetBatchSnapshot(
      pendingRequests = 2, pendingBytes = 2048L, accepted = 10L, rejected = 3L, completed = 8L,
      failed = 1L, batches = 4L, batchRequests = 8L, queueNanos = 1500000000L)))
    assert(output.contains("cascade_offset_batch_pending_requests{node_id=\"7\"} 2.0\n"))
    assert(output.contains("cascade_offset_batch_rejected_total{node_id=\"7\"} 3.0\n"))
    assert(output.contains("cascade_offset_batch_queue_seconds_total{node_id=\"7\"} 1.5\n"))
    assert(!output.contains("group_id="))
    assert(!output.contains("principal="))
  }

  test("snapshot efficiency exports measured values with node-only labels and correct time units") {
    val output = PrometheusMetrics.encode(snapshot.copy(coordinator = cascade.coordinator.CoordinatorMetricsSnapshot.Empty.copy(
      encodedShards = 2L, reusedShards = 127L, encodedBytes = 1024L, preparationNanos = 1500000000L)))
    assert(output.contains("cascade_coordinator_snapshot_encoded_shards_total{node_id=\"7\"} 2.0\n"))
    assert(output.contains("cascade_coordinator_snapshot_reused_shards_total{node_id=\"7\"} 127.0\n"))
    assert(output.contains("cascade_coordinator_snapshot_encoded_bytes_total{node_id=\"7\"} 1024.0\n"))
    assert(output.contains("cascade_coordinator_snapshot_preparation_seconds_total{node_id=\"7\"} 1.5\n"))
    assert(!output.contains("group_id="))
    assert(!output.contains("transactional_id="))
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
        MechanismAuthenticationSnapshot("OAUTHBEARER", 8L, 9L),
        MechanismAuthenticationSnapshot("UNKNOWN", 0L, 7L)
      )
    ),
    tlsReload = TlsReloadSnapshot(enabled = true, generation = 4L, successfulReloads = 3L, failedReloads = 2L),
    trafficQuotas = TrafficQuotaSnapshot(
      RequestQuotaSnapshot(4L, 5L, 600L, 2),
      RequestQuotaSnapshot(10L, 11L, 1200L, 3),
      RequestQuotaSnapshot(14L, 15L, 1600L, 4),
      RequestQuotaSnapshot(19L, 20L, 2100L, 5)
    )
  )

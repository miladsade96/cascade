package cascade.e2e

import cascade.qualification.CoordinatorScaleQualification
import munit.FunSuite

final class PersistentCoordinatorEndToEndSuite extends FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(90L, "seconds")

  test("long-lived clients preserve offsets through failover and restart using shard objects") {
    val result = CoordinatorScaleQualification.run(60, 6, 2, persistent = true)
    assertEquals(result.clientsCreated, 60)
    assertEquals(result.clientLifecycle, "persistent")
    assertEquals(result.writes, 180)
    assertEquals(result.verified, 60)
    assertEquals(result.owners, Vector(1, 2, 3))
    assert(result.controllerFailover && result.restartRecovery)
    assert(result.warmupSeconds > 0d)
    assert(result.objectsWritten > 0L)
    assert(result.objectWrittenBytes > 0L)
    assert(result.journalDeltaBytes > 0L)
    assertEquals(result.maxConnectionsPerIp, 1000)
    assertEquals(result.rejectedConnections, 0L)
    assertEquals(result.batchRejected, 0L)
    assert(result.batchedRequests > result.batches)
    // Retained work can briefly outlive a client response; the configured admission cap is the invariant.
    assert(result.batchPeakRequests > 0 && result.batchPeakRequests <= cascade.group.OffsetBatchConfig().maxPendingRequests)
    assert(result.json.contains("\"warmup_writes\":60"))
    assert(result.snapshotEncodedShards > 0L)
    assert(result.snapshotReusedShards > result.snapshotEncodedShards)
    assert(result.snapshotEncodedBytes > 0L)
    assert(result.snapshotPreparationNanos > 0L)
  }

  test("persistent cardinality is bounded before starting brokers or allocating clients") {
    intercept[IllegalArgumentException](CoordinatorScaleQualification.run(2001, 1, 1, persistent = true))
  }

  test("single-request control preserves the same failover and restart contract") {
    val result = CoordinatorScaleQualification.run(30, 6, 1, persistent = true,
      batchConfig = cascade.group.OffsetBatchConfig(maxRequests = 1, lingerMillis = 0L))
    assertEquals(result.verified, 30)
    assertEquals(result.writes, 60)
    assertEquals(result.batchMaxRequests, 1)
    assertEquals(result.batchedRequests, result.batches)
    assertEquals(result.batchRejected, 0L)
    assert(result.controllerFailover && result.restartRecovery)
  }

  test("resident client connection budgets include bootstrap metadata and coordinator sockets") {
    assertEquals(CoordinatorScaleQualification.connectionLimit(60, persistent = true), 1000)
    assertEquals(CoordinatorScaleQualification.connectionLimit(1000, persistent = true), 3032)
    assertEquals(CoordinatorScaleQualification.connectionLimit(2000, persistent = true), 6032)
    assertEquals(CoordinatorScaleQualification.connectionLimit(1000, persistent = false), 1000)
  }

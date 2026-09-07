package cascade.qualification

import java.time.Instant
import munit.FunSuite

final class CoordinatorScaleReportSuite extends FunSuite:
  test("serializes acknowledged read evidence in the stable qualification schema") {
    val report = CoordinatorScaleReport(
      groups = 1000,
      concurrency = 32,
      rounds = 2,
      verified = 1000,
      writes = 3000,
      seconds = 30d,
      p50Millis = 10d,
      p95Millis = 20d,
      p99Millis = 30d,
      checkpointAttempts = 100L,
      checkpointFailures = 2L,
      deltaBytes = 3L,
      fullImageBytes = 4L,
      owners = Vector(1, 2, 3),
      controllerFailover = true,
      restartRecovery = true,
      revision = "a" * 40,
      startedAt = Instant.parse("2026-09-07T00:00:00Z"),
      journalDeltaBytes = 5L,
      journalFullBytes = 6L,
      journalCheckpointBytes = 7L,
      replicationDeltaBytes = 8L,
      replicationFullBytes = 9L,
      replicationFallbacks = 0L,
      offsetReadSnapshots = 3000L,
      offsetReadKeys = 3000L,
      stableOffsetSnapshots = 40L,
      transactionVisibilitySnapshots = 80L
    )

    val json = report.json
    assert(json.startsWith("{\"status\":\"passed\""))
    assert(json.endsWith("}"))
    assert(json.contains("\"owner_ids\":[1,2,3]"))
    assert(json.contains("\"writes_per_second\":100.000"))
    assert(json.contains("\"offset_read_snapshots\":3000"))
    assert(json.contains("\"offset_read_keys\":3000"))
    assert(json.contains("\"stable_offset_snapshots\":40"))
    assert(json.contains("\"transaction_visibility_snapshots\":80"))
    assertEquals("\"offset_read_snapshots\"".r.findAllIn(json).size, 1)
  }

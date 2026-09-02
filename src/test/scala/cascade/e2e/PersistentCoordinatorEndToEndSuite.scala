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
    assert(result.json.contains("\"warmup_writes\":60"))
  }

  test("persistent cardinality is bounded before starting brokers or allocating clients") {
    intercept[IllegalArgumentException](CoordinatorScaleQualification.run(2001, 1, 1, persistent = true))
  }

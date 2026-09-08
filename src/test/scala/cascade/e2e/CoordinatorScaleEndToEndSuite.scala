package cascade.e2e

import cascade.qualification.CoordinatorScaleQualification
import munit.FunSuite

final class CoordinatorScaleEndToEndSuite extends FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(120, "seconds")
  test("concurrent Kafka groups preserve exact offsets across owner loss and full restart") {
    val report = CoordinatorScaleQualification.run(60, 6, 2)
    assertEquals(report.verified, 60)
    assertEquals(report.writes, 180)
    assertEquals(report.owners, Vector(1, 2, 3))
    assert(report.controllerFailover && report.restartRecovery)
    assert(report.deltaBytes < report.fullImageBytes, report)
    assertEquals(report.quorumRejected, 0L)
    assertEquals(report.quorumFailed, 0L)
    assert(report.quorumCommitted > 0L, report)
    assert(report.quorumFinalizeMessages >= report.quorumCommitted * 2L, report)
    assert(report.shardJournalRecords > 0L, report)
    assert(report.shardJournalBytes > 0L, report)
    assertEquals(report.shardJournalTruncatedBytes, 0L)
    assert(report.json.contains("\"quorum_committed\""))
  }

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
    assertEquals(report.publicationRejected, 0L)
    assert(report.publicationPeakRequests <= 1024, report)
    assert(report.publicationPeakBytes <= 64L * 1024 * 1024, report)
    assert(report.publicationBatchRequests >= report.publicationCommittedRequests, report)
    assert(report.publicationCommittedRequests > 0L, report)
    assert(report.json.contains("\"publication_committed_requests\""))
  }

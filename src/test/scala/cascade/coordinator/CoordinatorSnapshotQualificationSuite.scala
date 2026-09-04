package cascade.coordinator

import cascade.qualification.CoordinatorSnapshotQualification
import munit.FunSuite

final class CoordinatorSnapshotQualificationSuite extends FunSuite:
  test("paired snapshot qualification verifies bytes and alternates measurement order") {
    val samples = CoordinatorSnapshotQualification.run(16, 20)
    assertEquals(samples.map(_.mode), Vector("full", "cached", "cached", "full", "full", "cached", "cached", "full"))
    assert(samples.forall(_.millis >= 0d))
    assert(samples.filter(_.mode == "cached").forall(_.reused > 0L))
    assert(samples.forall(s => s.encoded + s.reused == 20L * 129L))
  }

  test("invalid benchmark sizes fail before allocating workload state") {
    intercept[IllegalArgumentException](CoordinatorSnapshotQualification.run(0, 1))
    intercept[IllegalArgumentException](CoordinatorSnapshotQualification.run(10001, 1))
    intercept[IllegalArgumentException](CoordinatorSnapshotQualification.run(1, 10001))
  }

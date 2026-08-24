package cascade.security

import munit.FunSuite

final class RequestAdmissionSuite extends FunSuite:
  test("sheds overload immediately and returns request permits exactly once") {
    val admission = RequestAdmission(maxInFlight = 2)
    val first = admission.tryAcquire().getOrElse(fail("first request rejected"))
    val second = admission.tryAcquire().getOrElse(fail("second request rejected"))
    assertEquals(admission.tryAcquire(), None)
    assertEquals(admission.snapshot, RequestAdmissionSnapshot(active = 2, rejected = 1L))

    first.close()
    first.close()
    val replacement = admission.tryAcquire().getOrElse(fail("released permit was not reusable"))
    assertEquals(admission.snapshot.active, 2)

    second.close()
    replacement.close()
    assertEquals(admission.snapshot.active, 0)
  }

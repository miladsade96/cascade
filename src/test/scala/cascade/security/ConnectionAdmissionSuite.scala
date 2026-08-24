package cascade.security

import munit.FunSuite

final class ConnectionAdmissionSuite extends FunSuite:
  test("enforces global and per-IP connection caps without leaking leases") {
    val admission = ConnectionAdmission(maxConnections = 3, maxConnectionsPerIp = 2)
    val first = admission.tryAcquire("10.0.0.1").getOrElse(fail("first lease rejected"))
    val second = admission.tryAcquire("10.0.0.1").getOrElse(fail("second lease rejected"))
    assertEquals(admission.tryAcquire("10.0.0.1"), None)
    val third = admission.tryAcquire("10.0.0.2").getOrElse(fail("third lease rejected"))
    assertEquals(admission.tryAcquire("10.0.0.3"), None)

    first.close()
    first.close()
    val replacement = admission.tryAcquire("10.0.0.3").getOrElse(fail("released capacity was not reusable"))
    assertEquals(admission.snapshot.active, 3)
    assertEquals(admission.snapshot.rejected, 2L)

    second.close()
    third.close()
    replacement.close()
    assertEquals(admission.snapshot.active, 0)
    assertEquals(admission.snapshot.activeByIp, Map.empty)
  }

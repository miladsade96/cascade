package cascade.qualification

import munit.FunSuite

final class RollingTrafficLedgerSuite extends FunSuite:
  test("rolling traffic ledger plans globally contiguous phase values") {
    val ledger = RollingTrafficLedger()
    val first = ledger.reserve("baseline", 2)
    val second = ledger.reserve("upgrade", 2)
    assertEquals(first, Vector(0L -> "0:baseline:0", 1L -> "1:baseline:1"))
    assertEquals(second, Vector(2L -> "2:upgrade:0", 3L -> "3:upgrade:1"))
    assertEquals(ledger.size, 4)
    ledger.assertAcknowledged(first ++ second, Vector(0L, 1L, 2L, 3L))
    ledger.assertObserved((first ++ second).map((offset, value) => offset -> value))
  }

  test("rolling traffic ledger rejects duplicates, omissions and reorder") {
    val ledger = RollingTrafficLedger()
    val planned = ledger.reserve("transition", 3)
    intercept[IllegalArgumentException](ledger.assertAcknowledged(planned, Vector(0L, 1L))): Unit
    intercept[IllegalArgumentException](ledger.assertAcknowledged(planned, Vector(0L, 2L, 1L))): Unit
    intercept[IllegalArgumentException](ledger.assertObserved(Vector(0L -> planned.head._2, 1L -> planned(1)._2))): Unit
    intercept[IllegalArgumentException](ledger.assertObserved(Vector(
      0L -> planned.head._2, 2L -> planned(1)._2, 1L -> planned(2)._2
    ))): Unit
  }

package cascade.qualification

final class DisasterRecoveryLedgerSuite extends munit.FunSuite:
  test("the disaster-recovery checksum is deterministic and count-sensitive") {
    val first = DisasterRecoveryLedger.checksumFor(25)
    assertEquals(first.length, 64)
    assertEquals(first, DisasterRecoveryLedger.checksumFor(25))
    assertNotEquals(first, DisasterRecoveryLedger.checksumFor(24))
    assertEquals(
      DisasterRecoveryLedger.checksumFor(0),
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
  }

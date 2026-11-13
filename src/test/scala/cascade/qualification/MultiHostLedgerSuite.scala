package cascade.qualification

final class MultiHostLedgerSuite extends munit.FunSuite:
  test("the multi-host ledger checksum is deterministic and covers every record") {
    val first = MultiHostLedger.checksumFor(600)
    assertEquals(first.length, 64)
    assertEquals(first, MultiHostLedger.checksumFor(600))
    assertNotEquals(first, MultiHostLedger.checksumFor(599))
  }

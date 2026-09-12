package cascade.coordinator

import munit.FunSuite

final class CoordinatorQuorumConfigSuite extends FunSuite:
  test("accepts bounded quorum admission settings") {
    assertEquals(CoordinatorQuorumConfig().maxInflightTransactions, 256)
    assertEquals(CoordinatorQuorumConfig(32, 1500L).admissionTimeoutMillis, 1500L)
  }

  test("rejects unbounded quorum admission settings") {
    intercept[IllegalArgumentException](CoordinatorQuorumConfig(maxInflightTransactions = 0))
    intercept[IllegalArgumentException](CoordinatorQuorumConfig(maxInflightTransactions = 65537))
    intercept[IllegalArgumentException](CoordinatorQuorumConfig(admissionTimeoutMillis = 0L))
    intercept[IllegalArgumentException](CoordinatorQuorumConfig(admissionTimeoutMillis = 60001L))
    intercept[IllegalArgumentException](CoordinatorQuorumConfig(resolutionIntervalMillis = 99L))
    intercept[IllegalArgumentException](CoordinatorQuorumConfig(resolutionIntervalMillis = 2000L, resolutionDelayMillis = 1000L))
    intercept[IllegalArgumentException](CoordinatorQuorumConfig(journalCompactionBytes = 1000L))
  }

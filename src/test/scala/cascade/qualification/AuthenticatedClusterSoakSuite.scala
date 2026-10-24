package cascade.qualification

import cascade.security.SecurityTestSupport
import java.nio.file.Files
import java.time.Instant

final class AuthenticatedClusterSoakSuite extends munit.FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(120L, "seconds")

  test("authenticated RF=3 smoke preserves exact transactions through rotation, rebalance, failover, and restart") {
    val report = AuthenticatedClusterSoak.run(
      SoakConfig(
        durationSeconds = 1L,
        tenants = 2,
        recordsPerCycle = 2,
        payloadBytes = 64,
        reportIntervalSeconds = 1L,
        profile = SoakProfile.AuthenticatedCluster,
        rebalanceIntervalSeconds = 1L,
        churnIntervalSeconds = 1L,
        tlsRotationAfterSeconds = 1L,
        abortEveryCycles = 1,
        finalRestart = true,
        quotaBytesPerSecond = 1024L * 1024
      )
    )

    assert(report.passed, report.json)
    assert(report.cycles >= 1L)
    assertEquals(report.records, report.cycles * 2L * 2L)
    assertEquals(report.counters.verifiedRecords, report.records)
    assertEquals(report.counters.recoveredRecords, report.records)
    assertEquals(report.counters.abortedRecords, report.cycles * 2L)
    assertEquals(report.counters.tlsRotations, 1L)
    assertEquals(report.counters.rebalances, 1L)
    assertEquals(report.counters.brokerRestarts, 1L)
    assertEquals(report.counters.controllerFailovers, 1L)
    assert(report.counters.quotaReservations > 0L)
    assert(report.counters.authenticationSuccesses > 0L)
    assertEquals(report.counters.authenticatedTenants, 2)
    assert(report.counters.restartVerified)
  }

  test("soak evidence is atomically replaced and safely escapes failures") {
    val directory = Files.createTempDirectory("cascade-soak-evidence")
    val reportPath = directory.resolve("report.json")
    try
      val now = Instant.parse("2026-10-24T00:00:00Z")
      val first = SoakReport(
        SoakProfile.AuthenticatedCluster,
        now,
        now.plusSeconds(1L),
        1L,
        1d,
        2,
        "a" * 40,
        SoakCounters(records = 4L, verifiedRecords = 4L, cycles = 1L)
      )
      SoakEvidence.writeAtomically(reportPath, first)
      assert(Files.readString(reportPath).contains("\"status\":\"passed\""))

      val failed = first.copy(failure = Some("line one\n\"unsafe\""))
      SoakEvidence.writeAtomically(reportPath, failed)
      val content = Files.readString(reportPath)
      assert(content.contains("\"status\":\"failed\""))
      assert(content.contains("line one\\n\\\"unsafe\\\""))
      val files = Files.list(directory)
      try assertEquals(files.toArray.length, 1)
      finally files.close()
    finally SecurityTestSupport.deleteTree(directory)
  }

package cascade.security

import cascade.protocol.Errors
import java.util.concurrent.{Callable, Executors, TimeUnit}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class ClusterQuotaLedgerSuite extends FunSuite:
  private val resources = ResourceLimits(requestBytesPerSecond = 1000, requestBurstBytes = 1000, maxThrottleMillis = 2000)
  private val limit = QuotaLimit.forKind(resources, QuotaKind.Request)

  test("reclaims capacity that no broker reserved") {
    var now = 0L
    val ledger = ClusterQuotaLedger(resources, () => now)
    assert(ledger.activateTerm(1))
    now = 1_000_000_000L

    assertEquals(reserve(ledger, "tenant-a", 800).decision, Some(QuotaDecision.Allowed))
    assertEquals(reserve(ledger, "tenant-a", 200).decision, Some(QuotaDecision.Allowed))
    assertEquals(reserve(ledger, "tenant-a", 1).decision, Some(QuotaDecision.Throttle(1)))
    assertEquals(ledger.snapshot.reservations, 3L)
  }

  test("isolates global buckets by principal and quota kind") {
    var now = 0L
    val configured = resources.copy(produceBytesPerSecond = 1000, produceBurstBytes = 1000)
    val ledger = ClusterQuotaLedger(configured, () => now)
    assert(ledger.activateTerm(1))
    now = 1_000_000_000L

    assertEquals(reserve(ledger, "tenant-a", 1000).decision, Some(QuotaDecision.Allowed))
    assertEquals(reserve(ledger, "tenant-b", 1000).decision, Some(QuotaDecision.Allowed))
    val produce = ClusterQuotaReservation(
      QuotaKind.Produce,
      "tenant-a",
      1000,
      rejectExcess = true,
      QuotaLimit.forKind(configured, QuotaKind.Produce),
      1
    )
    assertEquals(ledger.reserve(produce).decision, Some(QuotaDecision.Allowed))
    assertEquals(ledger.snapshot.principals, 2)
  }

  test("cold starts every newer controller term and rejects stale terms") {
    var now = 0L
    val ledger = ClusterQuotaLedger(resources, () => now)
    assert(ledger.activateTerm(4))
    now = 1_000_000_000L
    assertEquals(reserve(ledger, "tenant", 1000, term = 4).decision, Some(QuotaDecision.Allowed))

    assertEquals(reserve(ledger, "tenant", 1000, term = 5).decision, Some(QuotaDecision.Throttle(1000)))
    val stale = reserve(ledger, "tenant", 1, term = 4)
    assertEquals(stale.errorCode, Errors.NotController)
    assertEquals(stale.decision, None)
    assertEquals(ledger.snapshot.controllerTerm, 5L)
    assertEquals(ledger.snapshot.epochResets, 2L)
  }

  test("fails closed when broker quota settings differ") {
    val ledger = ClusterQuotaLedger(resources, () => 0L)
    val request = ClusterQuotaReservation(QuotaKind.Request, "tenant", 1, true, limit.copy(bytesPerSecond = 999), 1)
    val result = ledger.reserve(request)
    assertEquals(result.errorCode, Errors.InvalidConfiguration)
    assertEquals(result.decision, None)
    assertEquals(ledger.snapshot.configurationMismatches, 1L)
  }

  test("serializes concurrent reservations without overspending the global burst") {
    var now = 0L
    val ledger = ClusterQuotaLedger(resources, () => now)
    ledger.activateTerm(1): Unit
    now = 1_000_000_000L
    val executor = Executors.newVirtualThreadPerTaskExecutor()
    try
      val tasks = Vector.fill(20)(Callable(() => reserve(ledger, "tenant", 100).decision.get))
      val decisions = executor.invokeAll(tasks.asJava).asScala.map(_.get()).toVector
      assertEquals(decisions.count(_ == QuotaDecision.Allowed), 10)
      assertEquals(decisions.count(_.isInstanceOf[QuotaDecision.Throttle]), 10)
      assertEquals(ledger.snapshot.reservations, 20L)
    finally
      executor.shutdownNow(): Unit
      executor.awaitTermination(5, TimeUnit.SECONDS): Unit
  }

  private def reserve(
      ledger: ClusterQuotaLedger,
      principal: String,
      bytes: Int,
      term: Long = 1L
  ): ClusterQuotaResult =
    ledger.reserve(ClusterQuotaReservation(QuotaKind.Request, principal, bytes, rejectExcess = true, limit, term))

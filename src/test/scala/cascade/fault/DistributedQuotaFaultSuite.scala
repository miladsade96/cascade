package cascade.fault

import cascade.security.{QuotaDecision, QuotaKind, ResourceLimits}
import munit.FunSuite

final class DistributedQuotaFaultSuite extends FunSuite:
  test("an active broker reclaims unused cluster quota capacity") {
    val cluster = FaultCluster(
      3,
      resources = ResourceLimits(
        requestBytesPerSecond = 3000,
        requestBurstBytes = 3000,
        maxThrottleMillis = 5000
      )
    )
    try
      cluster.startAll()
      awaitDistributedQuotas(cluster)
      Thread.sleep(1100)
      val controller = cluster.broker(1).metricsSnapshot.controllerId
      val reservationsBefore = cluster.broker(controller).distributedQuotaSnapshot.reservations

      val decisions = Vector(
        cluster.broker(1).reserveClusterQuota(QuotaKind.Request, "User:tenant-a", 2000),
        cluster.broker(2).reserveClusterQuota(QuotaKind.Request, "User:tenant-a", 1000),
        cluster.broker(3).reserveClusterQuota(QuotaKind.Request, "User:tenant-a", 500)
      ).map(_.getOrElse(fail("distributed quota feature was not active")))

      assertEquals(decisions.take(2), Vector(QuotaDecision.Allowed, QuotaDecision.Allowed))
      assert(decisions.last.isInstanceOf[QuotaDecision.Throttle])
      assertEquals(cluster.broker(controller).distributedQuotaSnapshot.reservations, reservationsBefore + 3L)
      assert(cluster.runningNodeIds.filterNot(_ == controller).map(cluster.broker).exists(_.distributedQuotaSnapshot.forwarded > 0L))
    finally cluster.close()
  }

  test("a replacement controller cannot reuse the previous term's burst") {
    val cluster = FaultCluster(
      3,
      resources = ResourceLimits(
        requestBytesPerSecond = 1000,
        requestBurstBytes = 10000,
        maxThrottleMillis = 20000
      )
    )
    try
      cluster.startAll()
      awaitDistributedQuotas(cluster)
      val previousController = cluster.broker(1).metricsSnapshot.controllerId
      val previousTerm = cluster.broker(previousController).distributedQuotaSnapshot.controllerTerm
      cluster.stop(previousController)

      val nextController = awaitController(cluster, previousController)
      val requester = cluster.runningNodeIds.head
      val decision = cluster.broker(requester)
        .reserveClusterQuota(QuotaKind.Request, "User:failover-tenant", 10000)
        .getOrElse(fail("distributed quotas became inactive after controller failover"))

      assert(decision.isInstanceOf[QuotaDecision.Throttle])
      assert(cluster.broker(nextController).distributedQuotaSnapshot.controllerTerm > previousTerm)
    finally cluster.close()
  }

  private def awaitDistributedQuotas(cluster: FaultCluster): Unit =
    val deadline = System.nanoTime() + 15_000_000_000L
    var active = false
    while !active && System.nanoTime() < deadline do
      try active = cluster.broker(1).reserveClusterQuota(QuotaKind.Request, "User:activation-probe", 1).nonEmpty
      catch case _: IllegalStateException => ()
      if !active then Thread.sleep(50)
    assert(active, "distributed quota feature level 2 did not activate")

  private def awaitController(cluster: FaultCluster, excluded: Int): Int =
    val deadline = System.nanoTime() + 15_000_000_000L
    var elected = -1
    while elected < 0 && System.nanoTime() < deadline do
      elected = cluster.runningNodeIds.iterator.flatMap { nodeId =>
        val snapshot = cluster.broker(nodeId).metricsSnapshot
        Option.when(snapshot.controllerId != excluded && cluster.runningNodeIds.contains(snapshot.controllerId) && !snapshot.brokerFenced)(
          snapshot.controllerId
        )
      }.nextOption().getOrElse(-1)
      if elected < 0 then Thread.sleep(50)
    if elected < 0 then fail("replacement controller was not elected")
    elected

package cascade.cluster

import cascade.coordinator.CoordinatorProbe
import cascade.fault.{FaultCluster, FaultSelector}
import cascade.protocol.Errors
import munit.FunSuite

final class MetadataDeltaFaultSuite extends FunSuite:
  test("a lagging follower rejects the delta base and triggers full snapshot recovery") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val follower = cluster.nodes.find(_.id != controller.id).get
      val deltaLink = FaultSelector(controller.id, follower.id, Some(InternalApi.MetadataDeltaCommit))
      cluster.faults.block(deltaLink)
      cluster.faults.block(FaultSelector(controller.id, follower.id, Some(InternalApi.MetadataCommit)))
      cluster.faults.block(FaultSelector(follower.id, controller.id, Some(InternalApi.MetadataSnapshot)))
      val initial = CoordinatorProbe.snapshot(controller)._3
      assertEquals(CoordinatorProbe.commit(controller, MetadataDeltaFixture.update(initial, "missed", 1L)._1.change), Errors.None)
      cluster.faults.unblock(deltaLink)
      val base = CoordinatorProbe.snapshot(controller)._3
      assertEquals(CoordinatorProbe.commit(controller, MetadataDeltaFixture.update(base, "following", 2L)._1.change), Errors.None)
      assert(cluster.broker(controller.id).metricsSnapshot.metadataTransfers.fallbacks > 0L)
      val expected = CoordinatorProbe.snapshot(controller)._3.coordinator
      cluster.faults.heal()
      val deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(15L)
      var recovered = CoordinatorProbe.snapshot(follower)._3.coordinator
      while recovered != expected && System.nanoTime() < deadline do
        Thread.sleep(25L)
        recovered = CoordinatorProbe.snapshot(follower)._3.coordinator
      assertEquals(recovered, expected)
    finally cluster.close()
  }

  test("missing both delta commit acknowledgements never reports a successful quorum write") {
    val cluster = FaultCluster(3)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val initial = CoordinatorProbe.snapshot(controller)._3
      cluster.nodes.filterNot(_.id == controller.id).foreach { follower =>
        cluster.faults.block(FaultSelector(controller.id, follower.id, Some(InternalApi.MetadataDeltaCommit)))
      }
      val proposed = MetadataDeltaFixture.update(initial, "rejected", 99L)._1.change
      assertNotEquals(CoordinatorProbe.commit(controller, proposed), Errors.None)
      assertEquals(CoordinatorProbe.snapshot(controller)._3.coordinator, initial.coordinator)
      cluster.faults.heal()
    finally cluster.close()
  }

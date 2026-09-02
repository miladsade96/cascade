package cascade.cluster

import cascade.coordinator.CoordinatorProbe
import cascade.fault.FaultCluster
import cascade.protocol.Errors
import java.nio.file.Files
import munit.FunSuite

final class MetadataFailureFencingSuite extends FunSuite:
  test("a real broker fences coordinator service after checkpoint publication fails") {
    val cluster = FaultCluster(3, peerTimeoutMillis = 1500, heartbeatMillis = 250,
      electionTimeoutMillis = 5000, journalCompactionBytes = 1024L)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val blocker = cluster.directories(controller.id - 1).resolve(".cascade/cluster-metadata.log.cleaned")
      Files.createDirectory(blocker)
      Files.writeString(blocker.resolve("fault"), "prevent checkpoint publication")
      var attempts = 0
      while !cluster.broker(controller.id).metricsSnapshot.brokerFenced && attempts < 30 do
        val state = CoordinatorProbe.snapshot(controller)._3
        val delta = MetadataDeltaFixture.update(state, "fencing-worker", attempts.toLong)._1
        try CoordinatorProbe.commit(controller, delta.change): Unit
        catch case _: Exception => () // A failed publication can close the request without a reply.
        attempts += 1
      assert(cluster.broker(controller.id).metricsSnapshot.brokerFenced)
      val state = CoordinatorProbe.snapshot(controller)._3
      assertEquals(CoordinatorProbe.commit(controller, MetadataDeltaFixture.update(state, "fencing-worker", 999L)._1.change), Errors.NotController)
      Files.delete(blocker.resolve("fault"))
      Files.delete(blocker)
    finally cluster.close()
  }

package cascade.cluster

import cascade.coordinator.CoordinatorProbe
import cascade.fault.FaultCluster
import cascade.protocol.{ByteCursor, Errors}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class MetadataHeartbeatPublicationSuite extends FunSuite:
  test("heartbeat reconciliation waits for a follower-first metadata publication to settle") {
    val cluster = FaultCluster(3, peerTimeoutMillis = 3000, heartbeatMillis = 100, electionTimeoutMillis = 10000)
    val publisher = Executors.newSingleThreadExecutor()
    val persisted = CountDownLatch(2)
    val release = CountDownLatch(1)
    val aheadHeartbeat = CountDownLatch(1)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val (term, _, base) = CoordinatorProbe.snapshot(controller)
      val (delta, expected) = MetadataDeltaFixture.update(base, "publication-race", 42L)
      cluster.faults.observeReplies { (call, bytes) =>
        if call.sourceId == controller.id && call.apiKey == InternalApi.MetadataDeltaCommit then
          persisted.countDown()
          if !release.await(2L, TimeUnit.SECONDS) then throw IllegalStateException("publication barrier timed out")
        else if call.sourceId == controller.id && call.apiKey == InternalApi.ControllerHeartbeat && persisted.getCount == 0L then
          val response = ByteCursor(bytes)
          val responseTerm = response.readLong()
          val error = response.readShort()
          val metadataTerm = response.readLong()
          val version = response.readLong()
          if error == Errors.None && responseTerm == term && metadataTerm == term && version == expected.version then
            aheadHeartbeat.countDown()
      }
      val result = publisher.submit(new java.util.concurrent.Callable[Short]:
        override def call(): Short = CoordinatorProbe.commit(controller, delta.change)
      )
      assert(persisted.await(2L, TimeUnit.SECONDS), "followers must persist before releasing their acknowledgements")
      assert(aheadHeartbeat.await(1L, TimeUnit.SECONDS), "a heartbeat must observe the unpublished follower position")
      // Inspect a real contending monitor, not a sleep-based assumption about when it reconciled.
      def reconciliationBlocked: Boolean = Thread.getAllStackTraces.asScala.exists { case (thread, stack) =>
        thread.getName == "cascade-cluster-monitor" && thread.getState == Thread.State.BLOCKED &&
          stack.headOption.exists(_.getMethodName == "sendHeartbeats")
      }
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L)
      while !reconciliationBlocked && System.nanoTime() < deadline do Thread.sleep(5L)
      assert(reconciliationBlocked, "the heartbeat must reconcile only after the proposal releases the mutation lock")
      assertEquals(CoordinatorProbe.snapshot(controller)._2, controller.id)
      release.countDown()
      assertEquals(result.get(5L, TimeUnit.SECONDS), Errors.None)
      val (committedTerm, committedController, committed) = CoordinatorProbe.snapshot(controller)
      assertEquals(committedTerm, term)
      assertEquals(committedController, controller.id)
      assertEquals(committed.coordinator, expected.coordinator)
    finally
      release.countDown()
      cluster.faults.heal()
      publisher.shutdownNow(): Unit
      publisher.awaitTermination(5L, TimeUnit.SECONDS): Unit
      cluster.close()
  }

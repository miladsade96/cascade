package cascade.coordinator

import cascade.protocol.Errors
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import munit.FunSuite

final class CoordinatorDeltaBatcherSuite extends FunSuite:
  test("coalesces concurrent compatible proposals and accounts retained work") {
    val dispatched = new java.util.concurrent.ConcurrentLinkedQueue[Int]()
    val batcher = CoordinatorDeltaBatcher(
      CoordinatorPublicationConfig(maxRequests = 8, lingerMillis = 40L, queueTimeoutMillis = 2000L),
      deltas =>
        dispatched.add(deltas.size): Unit
        Vector.fill(deltas.size)(Errors.None)
    )
    val executor = Executors.newFixedThreadPool(8)
    val start = CountDownLatch(1)
    try
      val results = (0 until 8).map { index =>
        executor.submit[Short](() => { start.await(); batcher.submit(delta(index)) })
      }
      start.countDown()
      assertEquals(results.map(_.get(3L, TimeUnit.SECONDS)).toVector, Vector.fill(8)(Errors.None))
      val snapshot = batcher.snapshot
      assertEquals(dispatched.size(), 1)
      assertEquals(dispatched.peek(), 8)
      assertEquals(snapshot.pendingRequests, 0)
      assertEquals(snapshot.pendingBytes, 0L)
      assertEquals(snapshot.accepted, 8L)
      assertEquals(snapshot.completed, 8L)
      assertEquals(snapshot.committedBatches, 1L)
      assertEquals(snapshot.committedRequests, 8L)
      assert(snapshot.queueNanos > 0L)
    finally
      batcher.close()
      executor.shutdownNow(): Unit
  }

  private def delta(shard: Int, bytes: Int = 1): CoordinatorDelta =
    CoordinatorDelta(1L, Vector(CoordinatorShardUpdate(shard, 0L, Vector.fill(bytes)(shard.toByte))))

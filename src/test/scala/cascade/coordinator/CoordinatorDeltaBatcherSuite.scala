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

  test("rejects oversized and over-capacity proposals before publication") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val batcher = CoordinatorDeltaBatcher(
      CoordinatorPublicationConfig(maxRequests = 1, maxBytes = 1024L, maxPendingRequests = 1,
        maxPendingBytes = 1024L, lingerMillis = 0L, queueTimeoutMillis = 2000L),
      deltas =>
        entered.countDown()
        assert(release.await(2L, TimeUnit.SECONDS))
        Vector.fill(deltas.size)(Errors.None)
    )
    val executor = Executors.newSingleThreadExecutor()
    try
      assertEquals(batcher.submit(delta(0, 2000)), Errors.InvalidRequest)
      val admitted = executor.submit[Short](() => batcher.submit(delta(1)))
      assert(entered.await(1L, TimeUnit.SECONDS))
      assertEquals(batcher.submit(delta(2)), Errors.RequestTimedOut)
      release.countDown()
      assertEquals(admitted.get(2L, TimeUnit.SECONDS), Errors.None)
      val snapshot = batcher.snapshot
      assertEquals(snapshot.accepted, 1L)
      assertEquals(snapshot.rejected, 2L)
      assertEquals(snapshot.peakRequests, 1)
      assert(snapshot.peakBytes <= 1024L)
    finally
      release.countDown()
      batcher.close()
      executor.shutdownNow(): Unit
  }

  test("expires queued work without abandoning an active publication") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val calls = new java.util.concurrent.atomic.AtomicInteger()
    val batcher = CoordinatorDeltaBatcher(
      CoordinatorPublicationConfig(maxRequests = 1, maxPendingRequests = 2, lingerMillis = 0L, queueTimeoutMillis = 80L),
      deltas =>
        calls.incrementAndGet(): Unit
        entered.countDown()
        assert(release.await(2L, TimeUnit.SECONDS))
        Vector.fill(deltas.size)(Errors.None)
    )
    val executor = Executors.newFixedThreadPool(2)
    try
      val active = executor.submit[Short](() => batcher.submit(delta(0)))
      assert(entered.await(1L, TimeUnit.SECONDS))
      val queued = executor.submit[Short](() => batcher.submit(delta(1)))
      assertEquals(queued.get(1L, TimeUnit.SECONDS), Errors.RequestTimedOut)
      release.countDown()
      assertEquals(active.get(2L, TimeUnit.SECONDS), Errors.None)
      assertEquals(calls.get(), 1)
      val snapshot = batcher.snapshot
      assertEquals(snapshot.accepted, 2L)
      assertEquals(snapshot.failed, 1L)
      assertEquals(snapshot.completed, 2L)
    finally
      release.countDown()
      batcher.close()
      executor.shutdownNow(): Unit
  }

  private def delta(shard: Int, bytes: Int = 1): CoordinatorDelta =
    CoordinatorDelta(1L, Vector(CoordinatorShardUpdate(shard, 0L, Vector.fill(bytes)(shard.toByte))))

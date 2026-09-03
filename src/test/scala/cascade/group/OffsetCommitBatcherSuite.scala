package cascade.group

import cascade.protocol.Errors
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit, TimeoutException}
import java.util.concurrent.atomic.AtomicInteger
import munit.FunSuite

final class OffsetCommitBatcherSuite extends FunSuite:
  private def command(id: String): OffsetCommitCommand =
    OffsetCommitCommand(id, -1, "", None, Vector(OffsetCommitValue(
      GroupOffsetKey(id, "events", 0), CommittedOffset(1L, -1, None, 0L))))

  test("concurrent requests share publication without exceeding batch bounds") {
    val calls = AtomicInteger()
    val largest = AtomicInteger()
    val executor = Executors.newFixedThreadPool(16)
    val start = CountDownLatch(1)
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(maxRequests = 8, lingerMillis = 100), (commands, admission) =>
      calls.incrementAndGet()
      largest.accumulateAndGet(commands.size, (a, b) => math.max(a, b))
      commands.indices.map(admission).toVector,
      _ => true
    )
    try
      val results = (1 to 16).map(id => executor.submit[Short](() => { start.await(); batcher.commit(command(s"g$id")) }))
      start.countDown()
      results.foreach(result => assertEquals(result.get(5L, TimeUnit.SECONDS), Errors.None))
      assert(calls.get() < 16)
      assert(largest.get() > 1 && largest.get() <= 8)
      batcher.close()
      val measured = batcher.snapshot
      assertEquals(measured.accepted, 16L)
      assertEquals(measured.completed, 16L)
      assertEquals(measured.failed, 0L)
      assertEquals(measured.batchRequests, 16L)
      assertEquals(measured.batches, calls.get().toLong)
      assertEquals(measured.pendingRequests, 0)
      assertEquals(measured.pendingBytes, 0L)
      assert(measured.queueNanos > 0L)
    finally
      start.countDown()
      batcher.close()
      executor.close()
  }

  test("admission rechecks current ownership inside the publication callback") {
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(), (commands, admission) => commands.indices.map(admission).toVector,
      key => key != "moved")
    try
      assertEquals(batcher.commit(command("moved")), Errors.NotCoordinator)
      assertEquals(batcher.commit(command("local")), Errors.None)
    finally batcher.close()
  }

  test("a single oversized command is rejected without invoking publication") {
    val calls = AtomicInteger()
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(maxBytes = 1024L), (commands, _) =>
      calls.incrementAndGet()
      Vector.fill(commands.size)(Errors.None), _ => true)
    try
      assertEquals(batcher.commit(command("x" * 1024)), Errors.InvalidRequest)
      assertEquals(calls.get(), 0)
    finally batcher.close()
  }

  test("publication exceptions fail callers and do not strand the worker") {
    val calls = AtomicInteger()
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(), (commands, admission) =>
      if calls.incrementAndGet() == 1 then throw IllegalStateException("injected pre-publication failure")
      commands.indices.map(admission).toVector, _ => true)
    try
      assertEquals(batcher.commit(command("first")), Errors.CoordinatorNotAvailable)
      assertEquals(batcher.commit(command("second")), Errors.None)
    finally batcher.close()
  }

  for byteBound <- Vector(false, true) do
    test(s"pending admission includes in-flight requests: byte bound=$byteBound") {
      val entered = CountDownLatch(1)
      val release = CountDownLatch(1)
      val executor = Executors.newFixedThreadPool(1)
      val config = OffsetBatchConfig(maxRequests = 1, maxBytes = 1024L,
        maxPendingRequests = if byteBound then 8 else 1, maxPendingBytes = 1024L, lingerMillis = 0L)
      val batcher = OffsetCommitBatcher(config, (commands, admission) =>
        val results = commands.indices.map(admission).toVector
        entered.countDown()
        if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("test barrier timed out")
        results, _ => true)
      try
        val active = executor.submit[Short](() => batcher.commit(command("first")))
        assert(entered.await(5L, TimeUnit.SECONDS))
        assertEquals(batcher.commit(command("second")), Errors.RequestTimedOut)
        assertEquals(batcher.snapshot.rejected, 1L)
        assertEquals(batcher.snapshot.pendingRequests, 1)
        release.countDown()
        assertEquals(active.get(5L, TimeUnit.SECONDS), Errors.None)
      finally
        release.countDown()
        batcher.close()
        executor.close()
    }

  test("claimed requests that expire while waiting for the service lock cannot later mutate") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val mutations = AtomicInteger()
    val executor = Executors.newFixedThreadPool(1)
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(queueTimeoutMillis = 100L), (commands, admission) =>
      entered.countDown()
      if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("test barrier timed out")
      commands.indices.map { index =>
        val gate = admission(index)
        if gate == Errors.None then mutations.incrementAndGet(): Unit
        gate
      }.toVector, _ => true)
    try
      val result = executor.submit[Short](() => batcher.commit(command("expired")))
      assert(entered.await(5L, TimeUnit.SECONDS))
      assertEquals(result.get(5L, TimeUnit.SECONDS), Errors.RequestTimedOut)
      release.countDown()
      batcher.close()
      assertEquals(mutations.get(), 0)
      assertEquals(batcher.snapshot.failed, 1L)
      assertEquals(batcher.snapshot.pendingBytes, 0L)
    finally
      release.countDown()
      batcher.close()
      executor.close()
  }

  test("queue deadlines never abandon a publication that already passed admission") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(1)
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(queueTimeoutMillis = 100L), (commands, admission) =>
      val result = commands.indices.map(admission).toVector
      entered.countDown()
      if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("test barrier timed out")
      result, _ => true)
    try
      val result = executor.submit[Short](() => batcher.commit(command("active")))
      assert(entered.await(5L, TimeUnit.SECONDS))
      assertEquals(batcher.commit(command("queued")), Errors.RequestTimedOut)
      intercept[TimeoutException](result.get(20L, TimeUnit.MILLISECONDS))
      release.countDown()
      assertEquals(result.get(5L, TimeUnit.SECONDS), Errors.None)
    finally
      release.countDown()
      batcher.close()
      executor.close()
  }

  test("shutdown rejects pending and new work but drains an active publication") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val executor = Executors.newFixedThreadPool(3)
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(maxRequests = 1, lingerMillis = 0L), (commands, admission) =>
      val results = commands.indices.map(admission).toVector
      entered.countDown()
      if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("test barrier timed out")
      results, _ => true)
    try
      val active = executor.submit[Short](() => batcher.commit(command("active")))
      assert(entered.await(5L, TimeUnit.SECONDS))
      val pending = executor.submit[Short](() => batcher.commit(command("pending")))
      val closing = executor.submit[Unit](() => batcher.close())
      assertEquals(pending.get(5L, TimeUnit.SECONDS), Errors.CoordinatorNotAvailable)
      assertEquals(batcher.commit(command("after-close")), Errors.CoordinatorNotAvailable)
      intercept[TimeoutException](closing.get(20L, TimeUnit.MILLISECONDS))
      release.countDown()
      assertEquals(active.get(5L, TimeUnit.SECONDS), Errors.None)
      closing.get(5L, TimeUnit.SECONDS)
    finally
      release.countDown()
      batcher.close()
      executor.close()
  }

  test("interrupted callers retain the active publication barrier and recover interrupt status") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val finished = CountDownLatch(1)
    val result = AtomicInteger(-1)
    val interruptStatus = AtomicInteger()
    val batcher = OffsetCommitBatcher(OffsetBatchConfig(), (commands, admission) =>
      val results = commands.indices.map(admission).toVector
      entered.countDown()
      if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("test barrier timed out")
      results, _ => true)
    val caller = Thread.ofPlatform().start(() =>
      result.set(batcher.commit(command("active")).toInt)
      interruptStatus.set(if Thread.currentThread().isInterrupted then 1 else 0)
      finished.countDown()
    )
    try
      assert(entered.await(5L, TimeUnit.SECONDS))
      caller.interrupt()
      assert(!finished.await(20L, TimeUnit.MILLISECONDS))
      release.countDown()
      assert(finished.await(5L, TimeUnit.SECONDS))
      assertEquals(result.get(), Errors.None.toInt)
      assertEquals(interruptStatus.get(), 1)
    finally
      release.countDown()
      batcher.close()
      caller.join(5000L)
  }

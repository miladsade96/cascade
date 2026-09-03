package cascade.group

import cascade.protocol.Errors
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
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

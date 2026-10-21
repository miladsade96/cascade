package cascade.backup

import java.util.UUID
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import munit.FunSuite

final class SnapshotRequestBarrierSuite extends FunSuite:
  test("freeze drains admitted requests and blocks new work until release") {
    val barrier = SnapshotRequestBarrier()
    val snapshotId = UUID.randomUUID()
    val active = barrier.enter()
    val executor = Executors.newFixedThreadPool(2)
    val freezing = CountDownLatch(1)
    val entered = CountDownLatch(1)
    try
      val freeze = executor.submit(new Runnable:
        override def run(): Unit =
          freezing.countDown()
          barrier.freeze(snapshotId, 5000L)
      )
      assert(freezing.await(1L, TimeUnit.SECONDS))
      Thread.sleep(25L)
      assert(!freeze.isDone)
      active.close()
      freeze.get(1L, TimeUnit.SECONDS)
      val waiting = executor.submit(new Runnable:
        override def run(): Unit =
          val lease = barrier.enter()
          try entered.countDown()
          finally lease.close()
      )
      assert(!entered.await(50L, TimeUnit.MILLISECONDS))
      barrier.release(snapshotId)
      assert(entered.await(1L, TimeUnit.SECONDS))
      waiting.get(1L, TimeUnit.SECONDS)
    finally
      active.close()
      if barrier.isOwnedBy(snapshotId) then barrier.release(snapshotId)
      executor.close()
  }

  test("capture prevents lease expiry and a lost release expires afterward") {
    val now = AtomicLong(0L)
    val barrier = SnapshotRequestBarrier(() => now.get())
    val snapshotId = UUID.randomUUID()
    barrier.freeze(snapshotId, 10L)
    val capture = barrier.beginCapture(snapshotId)
    now.set(TimeUnit.SECONDS.toNanos(1L))
    assert(barrier.isOwnedBy(snapshotId))
    capture.close()
    assert(barrier.isOwnedBy(snapshotId))
    now.addAndGet(TimeUnit.MILLISECONDS.toNanos(11L))
    assert(!barrier.isOwnedBy(snapshotId))
    val request = barrier.enter()
    request.close()
  }

  test("rejects competing owners and non-owner release") {
    val barrier = SnapshotRequestBarrier()
    val first = UUID.randomUUID()
    val second = UUID.randomUUID()
    barrier.freeze(first, 1000L)
    intercept[IllegalStateException](barrier.freeze(second, 1000L)): Unit
    intercept[IllegalStateException](barrier.release(second)): Unit
    barrier.release(first)
  }

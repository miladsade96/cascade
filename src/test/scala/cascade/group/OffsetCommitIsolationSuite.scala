package cascade.group

import cascade.coordinator.CoordinatorCheckpoint
import cascade.protocol.Errors
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit, TimeoutException}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class OffsetCommitIsolationSuite extends FunSuite:
  test("offset commands cannot mutate a different group's state") {
    withCoordinator { coordinator =>
      val value = offset("other", 12L)
      assertEquals(coordinator.commitOffsets("workers", -1, "", Vector(value)), Errors.InvalidRequest)
      assertEquals(coordinator.fetchOffset(value.key), None)
    }
  }

  for accepted <- Vector(true, false) do
    test(s"offset readers wait for publication and observe only authoritative state: accepted=$accepted") {
      withCoordinator { coordinator =>
        val original = offset("workers", 1L)
        assertEquals(coordinator.commitOffsets("workers", -1, "", Vector(original)), Errors.None)
        val baseline = coordinator.snapshotBytes.toVector
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val readerStarted = CountDownLatch(1)
        val executor = Executors.newVirtualThreadPerTaskExecutor()
        coordinator.attachCheckpoint(new CoordinatorCheckpoint:
          override def commit(): Boolean =
            entered.countDown()
            assert(release.await(5L, TimeUnit.SECONDS))
            if !accepted then coordinator.installSnapshot(baseline)
            accepted
        )
        try
          val write = executor.submit(() => coordinator.commitOffsets("workers", -1, "", Vector(offset("workers", 2L))))
          assert(entered.await(5L, TimeUnit.SECONDS))
          val read = executor.submit(() =>
            readerStarted.countDown()
            (coordinator.fetchOffset(original.key), coordinator.allOffsets("workers"))
          )
          assert(readerStarted.await(5L, TimeUnit.SECONDS))
          intercept[TimeoutException](read.get(50L, TimeUnit.MILLISECONDS))
          release.countDown()
          assertEquals(write.get(5L, TimeUnit.SECONDS), if accepted then Errors.None else Errors.CoordinatorNotAvailable)
          val expected = if accepted then offset("workers", 2L).value else original.value
          assertEquals(read.get(5L, TimeUnit.SECONDS), (Some(expected), Vector(original.key -> expected)))
        finally
          release.countDown()
          executor.close()
      }
    }

  private def offset(group: String, value: Long): OffsetCommitValue =
    OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(value, -1, None, 1000L))

  private def withCoordinator(body: GroupCoordinator => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-offset-isolation")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"), durableLocal = false, scheduleExpiration = false)
    try body(coordinator)
    finally
      coordinator.close()
      val paths = Files.walk(directory)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

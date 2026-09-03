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

  test("a batch publishes once, preserves FIFO rewinds, and isolates invalid commands") {
    withCoordinator { coordinator =>
      var checkpoints = 0
      coordinator.attachCheckpoint(() => { checkpoints += 1; true })
      def command(group: String, value: Long) = OffsetCommitCommand(group, -1, "", None, Vector(offset(group, value)))
      val commands = Vector(command("a", 20), command("b", 30).copy(generationId = 9), command("a", 10), command("c", 40))
      val results = coordinator.commitOffsetBatch(commands, index => if index == 3 then Errors.NotCoordinator else Errors.None)
      assertEquals(results, Vector(Errors.None, Errors.UnknownMemberId, Errors.None, Errors.NotCoordinator))
      assertEquals(checkpoints, 1)
      assertEquals(coordinator.fetchOffset(offset("a", 0).key).map(_.offset), Some(10L))
      assertEquals(coordinator.fetchOffset(offset("b", 0).key), None)
      assertEquals(coordinator.fetchOffset(offset("c", 0).key), None)
    }
  }

  test("failed batch publication rolls every valid group back together") {
    withCoordinator { coordinator =>
      val baseline = coordinator.snapshotBytes.toVector
      coordinator.attachCheckpoint(() => { coordinator.installSnapshot(baseline); false })
      val commands = Vector("a", "b", "c").map(group => OffsetCommitCommand(group, -1, "", None, Vector(offset(group, 1))))
      assertEquals(coordinator.commitOffsetBatch(commands), Vector.fill(3)(Errors.CoordinatorNotAvailable))
      commands.foreach(command => assertEquals(coordinator.allOffsets(command.groupId), Vector.empty))
    }
  }

  test("empty or wholly rejected batches do not publish a checkpoint") {
    withCoordinator { coordinator =>
      coordinator.attachCheckpoint(() => fail("unexpected checkpoint"))
      assertEquals(coordinator.commitOffsetBatch(Vector.empty), Vector.empty)
      assertEquals(coordinator.commitOffsetBatch(Vector(OffsetCommitCommand("a", -1, "", None, Vector.empty))), Vector(Errors.None))
      assertEquals(coordinator.commitOffsetBatch(Vector(OffsetCommitCommand("", -1, "", None, Vector.empty))), Vector(Errors.InvalidGroupId))
    }
  }

  test("a queued static member is revalidated after replacement before any offset mutation") {
    withCoordinator { coordinator =>
      def join = JoinGroupCommand("workers", 10000, 10000, "", Some("instance"), "consumer",
        Vector(GroupProtocol("range", Array[Byte](1))), "batch-fencing")
      val original = coordinator.join(join)
      assertEquals(original.errorCode, Errors.None)
      assertEquals(coordinator.sync("workers", original.generationId, original.memberId, Some("instance"),
        Vector(original.memberId -> Array[Byte](1))).errorCode, Errors.None)
      val entered = CountDownLatch(1)
      val release = CountDownLatch(1)
      val executor = Executors.newSingleThreadExecutor()
      val batcher = OffsetCommitBatcher(OffsetBatchConfig(), (commands, admission) =>
        entered.countDown()
        if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("fencing barrier timed out")
        coordinator.commitOffsetBatch(commands, admission), _ => true)
      try
        val pending = executor.submit[Short](() => batcher.commit(OffsetCommitCommand("workers",
          original.generationId, original.memberId, Some("instance"), Vector(offset("workers", 99L)))))
        assert(entered.await(5L, TimeUnit.SECONDS))
        val replacement = coordinator.join(join)
        assertEquals(replacement.errorCode, Errors.None)
        assertNotEquals(replacement.memberId, original.memberId)
        release.countDown()
        assertEquals(pending.get(5L, TimeUnit.SECONDS), Errors.FencedInstanceId)
        assertEquals(coordinator.allOffsets("workers"), Vector.empty)
      finally
        release.countDown()
        batcher.close()
        executor.close()
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
        val executor = Executors.newFixedThreadPool(2)
        coordinator.attachCheckpoint(new CoordinatorCheckpoint:
          override def commit(): Boolean =
            entered.countDown()
            if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("publication barrier timed out")
            if !accepted then coordinator.installSnapshot(baseline)
            accepted
        )
        try
          val write = executor.submit[Short](() => coordinator.commitOffsets("workers", -1, "", Vector(offset("workers", 2L))))
          assert(entered.await(5L, TimeUnit.SECONDS))
          val read = executor.submit[(Option[CommittedOffset], Vector[(GroupOffsetKey, CommittedOffset)])](() =>
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

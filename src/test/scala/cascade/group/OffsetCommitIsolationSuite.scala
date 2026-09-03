package cascade.group

import cascade.coordinator.CoordinatorCheckpoint
import cascade.protocol.Errors
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
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

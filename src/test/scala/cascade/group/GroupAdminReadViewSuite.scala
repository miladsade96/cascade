package cascade.group

import cascade.coordinator.CoordinatorCheckpoint
import cascade.protocol.Errors
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class GroupAdminReadViewSuite extends FunSuite:
  test("indexes a classic group with its selected metadata and assignment") {
    val member = StoredMember(
      "member-1",
      Some("instance-1"),
      10000,
      30000,
      Vector(StoredProtocol("range", Vector[Byte](1, 2)), StoredProtocol("other", Vector[Byte](9))),
      "client-1",
      1000L,
      Vector[Byte](3, 4)
    )
    val group = StoredGroup(
      "workers",
      GroupStatus.Stable,
      4,
      member.memberId,
      "consumer",
      "range",
      2000L,
      Vector(member),
      Vector(member.memberId),
      Vector.empty
    )

    val view = GroupAdminReadView.from(GroupImage(7L, Vector(group), Vector.empty))
    val description = view.get("workers").getOrElse(fail("missing group"))

    assertEquals(view.imageVersion, 7L)
    assertEquals(description.state, "Stable")
    assertEquals(description.protocolType, "consumer")
    assertEquals(description.protocolData, "range")
    assertEquals(description.members.map(_.metadata), Vector(Vector[Byte](1, 2)))
    assertEquals(description.members.map(_.assignment), Vector(Vector[Byte](3, 4)))
    assertEquals(description.members.map(_.groupInstanceId), Vector(Some("instance-1")))
  }

  test("lists consumer-protocol and offset-only groups deterministically") {
    val consumer = StoredConsumerGroup(
      "z-consumer",
      3,
      Vector(StoredConsumerMember(
        "member-2",
        None,
        Some("rack-a"),
        30000,
        Vector("events"),
        "uniform",
        2,
        1000L,
        Vector.empty
      ))
    )
    val offset = OffsetCommitValue(
      GroupOffsetKey("a-offsets", "events", 0),
      CommittedOffset(10L, -1, None, 1000L)
    )

    val view = GroupAdminReadView.from(GroupImage(8L, Vector.empty, Vector(offset), Vector(consumer)))

    assertEquals(view.groups.map(_.groupId), Vector("a-offsets", "z-consumer"))
    assertEquals(view.get("a-offsets").map(_.state), Some("Empty"))
    assertEquals(view.get("a-offsets").map(_.protocolType), Some(""))
    assertEquals(view.get("z-consumer").map(_.state), Some("Stable"))
    assertEquals(view.get("z-consumer").map(_.protocolType), Some("consumer"))
    assertEquals(view.get("z-consumer").map(_.protocolData), Some("uniform"))
  }

  for accepted <- Vector(true, false) do
    test(s"admin reads expose only acknowledged checkpoints: accepted=$accepted") {
      val directory = Files.createTempDirectory("cascade-group-admin-view")
      val coordinator = GroupCoordinator(directory.resolve("offsets.log"), durableLocal = false, scheduleExpiration = false)
      val entered = CountDownLatch(1)
      val release = CountDownLatch(1)
      val executor = Executors.newSingleThreadExecutor()
      try
        assertEquals(coordinator.commitOffsets("existing", -1, "", Vector(offset("existing"))), Errors.None)
        val baseline = coordinator.snapshotBytes.toVector
        coordinator.attachCheckpoint(new CoordinatorCheckpoint:
          override def commit(): Boolean =
            entered.countDown()
            if !release.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("admin publication timed out")
            if !accepted then coordinator.installSnapshot(baseline)
            accepted
        )
        val write = executor.submit[Short](() =>
          coordinator.commitOffsets("new-group", -1, "", Vector(offset("new-group")))
        )
        assert(entered.await(5L, TimeUnit.SECONDS))

        val during = coordinator.adminView
        assert(during.contains("existing"))
        assert(!during.contains("new-group"))

        release.countDown()
        assertEquals(write.get(5L, TimeUnit.SECONDS), if accepted then Errors.None else Errors.CoordinatorNotAvailable)
        assertEquals(coordinator.adminView.contains("new-group"), accepted)
        assert(!during.contains("new-group"))
      finally
        release.countDown()
        executor.shutdownNow(): Unit
        executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
        coordinator.close()
        val paths = Files.walk(directory)
        try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
        finally paths.close()
    }

  private def offset(groupId: String): OffsetCommitValue =
    OffsetCommitValue(GroupOffsetKey(groupId, "events", 0), CommittedOffset(10L, -1, None, 1000L))

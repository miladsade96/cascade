package cascade.group

import cascade.coordinator.CoordinatorCheckpoint
import cascade.protocol.Errors
import java.nio.file.Files
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class GroupCoordinatorSuite extends FunSuite:
  test("classic member joins, synchronizes, heartbeats, and commits offsets") {
    val directory = Files.createTempDirectory("cascade-group-coordinator-test")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"))
    try
      val metadata = Array[Byte](1, 2, 3)
      val firstJoin = coordinator.join(command(memberId = "", metadata))
      assertEquals(firstJoin.errorCode, Errors.MemberIdRequired)
      assert(firstJoin.memberId.nonEmpty)

      val joined = coordinator.join(command(firstJoin.memberId, metadata))
      assertEquals(joined.errorCode, Errors.None)
      assertEquals(joined.generationId, 1)
      assertEquals(joined.leaderId, firstJoin.memberId)
      assertEquals(joined.members.map(_.memberId), Vector(firstJoin.memberId))

      val assignment = Array[Byte](4, 5, 6)
      val synced = coordinator.sync("workers", joined.generationId, joined.memberId, Vector(joined.memberId -> assignment))
      assertEquals(synced, SyncGroupResult(Errors.None, assignment))
      assertEquals(coordinator.heartbeat("workers", joined.generationId, joined.memberId), Errors.None)

      val key = GroupOffsetKey("workers", "events", 0)
      val value = CommittedOffset(12L, 1, Some("manual"), 1234L)
      assertEquals(
        coordinator.commitOffsets("workers", joined.generationId, joined.memberId, Vector(OffsetCommitValue(key, value))),
        Errors.None
      )
      assertEquals(coordinator.fetchOffset(key), Some(value))
      assertEquals(coordinator.leave("workers", joined.memberId), Errors.None)
    finally
      coordinator.close()
      deleteTree(directory)
  }

  test("a classic group and its offsets continue from an installed snapshot") {
    val directory = Files.createTempDirectory("cascade-group-snapshot-test")
    val source = GroupCoordinator(directory.resolve("source-offsets.log"), scheduleExpiration = false)
    val target = GroupCoordinator(directory.resolve("target-offsets.log"), scheduleExpiration = false)
    try
      val pending = source.join(command("", Array[Byte](1, 2)))
      val joined = source.join(command(pending.memberId, Array[Byte](1, 2)))
      val assignment = Array[Byte](7, 8, 9)
      assertEquals(
        source.sync("workers", joined.generationId, joined.memberId, Vector(joined.memberId -> assignment)).errorCode,
        Errors.None
      )
      val key = GroupOffsetKey("workers", "events", 0)
      val offset = CommittedOffset(88L, 2, Some("failover"), 1234L)
      assertEquals(
        source.commitOffsets("workers", joined.generationId, joined.memberId, Vector(OffsetCommitValue(key, offset))),
        Errors.None
      )

      target.installSnapshot(source.snapshotBytes.toVector)
      assertEquals(target.heartbeat("workers", joined.generationId, joined.memberId), Errors.None)
      val restoredSync = target.sync("workers", joined.generationId, joined.memberId, Vector.empty)
      assertEquals(restoredSync.errorCode, Errors.None)
      assertEquals(restoredSync.assignment.toVector, assignment.toVector)
      assertEquals(target.fetchOffset(key), Some(offset))
    finally
      source.close()
      target.close()
      deleteTree(directory)
  }

  test("a failed coordinator checkpoint restores the last acknowledged group image") {
    val directory = Files.createTempDirectory("cascade-group-checkpoint-test")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"), scheduleExpiration = false)
    try
      val pending = coordinator.join(command("", Array[Byte](1, 2, 3)))
      val acknowledged = coordinator.snapshotBytes.toVector
      coordinator.attachCheckpoint(new CoordinatorCheckpoint:
        override def commit(): Boolean =
          coordinator.installSnapshot(acknowledged)
          false
      )

      val rejected = coordinator.join(command(pending.memberId, Array[Byte](1, 2, 3)))
      assertEquals(rejected.errorCode, Errors.CoordinatorNotAvailable)

      coordinator.attachCheckpoint(CoordinatorCheckpoint.Local)
      val retried = coordinator.join(command(pending.memberId, Array[Byte](1, 2, 3)))
      assertEquals(retried.errorCode, Errors.None)
      assertEquals(retried.generationId, 1)
    finally
      coordinator.close()
      deleteTree(directory)
  }

  test("coordinator expiration removes stale offsets and survives restart") {
    val directory = Files.createTempDirectory("cascade-group-offset-expiration-test")
    val path = directory.resolve("offsets.log")
    val old = OffsetCommitValue(GroupOffsetKey("workers", "events", 0), CommittedOffset(10L, -1, None, 1000L))
    val recent = OffsetCommitValue(GroupOffsetKey("workers", "events", 1), CommittedOffset(20L, -1, None, 2500L))
    try
      val coordinator = GroupCoordinator(path, scheduleExpiration = false, offsetRetentionMillis = 1000L)
      try
        assertEquals(coordinator.commitOffsets("workers", -1, "", Vector(old, recent)), Errors.None)
        coordinator.expireNow(nowMillis = 3000L)
        assertEquals(coordinator.fetchOffset(old.key), None)
        assertEquals(coordinator.fetchOffset(recent.key), Some(recent.value))
      finally coordinator.close()

      val recovered = GroupCoordinator(path, scheduleExpiration = false, offsetRetentionMillis = 1000L)
      try
        assertEquals(recovered.fetchOffset(old.key), None)
        assertEquals(recovered.fetchOffset(recent.key), Some(recent.value))
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("coordinator automatically compacts a growing local offset journal") {
    val directory = Files.createTempDirectory("cascade-group-journal-compaction-test")
    val path = directory.resolve("offsets.log")
    val key = GroupOffsetKey("workers", "events", 0)
    try
      val coordinator = GroupCoordinator(
        path,
        scheduleExpiration = false,
        journalCompactionBytes = 1024L
      )
      try
        (1L to 50L).foreach { offset =>
          val value = OffsetCommitValue(key, CommittedOffset(offset, -1, None, offset * 1000L))
          assertEquals(coordinator.commitOffsets("workers", -1, "", Vector(value)), Errors.None)
        }
        assertEquals(coordinator.fetchOffset(key).map(_.offset), Some(50L))
      finally coordinator.close()
      assert(Files.size(path) < 1024L)

      val recovered = GroupCoordinator(path, scheduleExpiration = false)
      try assertEquals(recovered.fetchOffset(key).map(_.offset), Some(50L))
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def command(memberId: String, metadata: Array[Byte]): JoinGroupCommand =
    JoinGroupCommand(
      groupId = "workers",
      sessionTimeoutMillis = 10_000,
      rebalanceTimeoutMillis = 10_000,
      memberId = memberId,
      groupInstanceId = None,
      protocolType = "consumer",
      protocols = Vector(GroupProtocol("range", metadata)),
      clientId = "coordinator-test"
    )

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

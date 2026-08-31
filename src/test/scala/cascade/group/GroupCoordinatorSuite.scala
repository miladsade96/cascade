package cascade.group

import cascade.coordinator.CoordinatorCheckpoint
import cascade.protocol.Errors
import java.nio.file.Files
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class GroupCoordinatorSuite extends FunSuite:
  test("consumer protocol assigns on heartbeat and advances members without a join-sync barrier") {
    val directory = Files.createTempDirectory("cascade-consumer-heartbeat-test")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"), scheduleExpiration = false)
    try
      def heartbeat(memberId: String, epoch: Int, topics: Option[Vector[String]]) =
        coordinator.consumerHeartbeat(
          ConsumerHeartbeatCommand(
            "modern-workers",
            memberId,
            epoch,
            None,
            None,
            if epoch == 0 then 30_000 else -1,
            topics,
            None,
            if epoch == 0 then Some(Vector.empty) else None
          ),
          topic => if topic == "events" then 4 else 0,
          heartbeatIntervalMillis = 3000
        )

      val first = heartbeat("member-a", 0, Some(Vector("events")))
      assertEquals(first.errorCode, Errors.None)
      assertEquals(first.memberEpoch, 1)
      assertEquals(first.assignment.flatMap(_.headOption).map(_.partitions), Some(Vector(0, 1, 2, 3)))

      val second = heartbeat("member-b", 0, Some(Vector("events")))
      assertEquals(second.errorCode, Errors.None)
      assertEquals(second.memberEpoch, 2)
      assertEquals(second.assignment.flatMap(_.headOption).map(_.partitions), Some(Vector(1, 3)))

      val advanced = heartbeat("member-a", first.memberEpoch, None)
      assertEquals(advanced.errorCode, Errors.None)
      assertEquals(advanced.memberEpoch, 2)
      assertEquals(advanced.assignment.flatMap(_.headOption).map(_.partitions), Some(Vector(0, 2)))

      val left = heartbeat("member-b", -1, None)
      assertEquals(left.errorCode, Errors.None)
      val restored = heartbeat("member-a", advanced.memberEpoch, None)
      assertEquals(restored.assignment.flatMap(_.headOption).map(_.partitions), Some(Vector(0, 1, 2, 3)))
    finally coordinator.close()
  }

  test("consumer coordinator failover renews liveness and expiry keeps the complete assignment") {
    val directory = Files.createTempDirectory("cascade-consumer-expiry-test")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"), scheduleExpiration = false)
    val topicId = ConsumerTopicId.forName("events")
    val stored = StoredConsumerGroup(
      "modern-workers",
      2,
      Vector(
        StoredConsumerMember("member-a", None, None, 30_000, Vector("events"), "uniform", 2, 0L, Vector(ConsumerTopicPartitions(topicId, Vector(0, 2)))),
        StoredConsumerMember("member-b", None, None, 30_000, Vector("events"), "uniform", 2, 0L, Vector(ConsumerTopicPartitions(topicId, Vector(1, 3))))
      )
    )
    try
      coordinator.installSnapshot(GroupCodec.encode(GroupImage(2L, Vector.empty, Vector.empty, Vector(stored))).toVector)
      coordinator.expireNow(System.currentTimeMillis() + 44_000L)
      assertEquals(GroupCodec.decode(coordinator.snapshotBytes).consumerGroups.head.members.size, 2)

      Thread.sleep(100L)
      val heartbeat = ConsumerHeartbeatCommand(
        "modern-workers", "member-b", 2, None, None, -1, None, None, None
      )
      assertEquals(coordinator.consumerHeartbeat(heartbeat, _ => 4).errorCode, Errors.None)
      coordinator.expireNow(System.currentTimeMillis() + 44_950L)
      val recovered = coordinator.consumerHeartbeat(heartbeat, _ => 4)
      assertEquals(recovered.errorCode, Errors.None)
      assertEquals(recovered.memberEpoch, 3)
      assertEquals(recovered.assignment.flatMap(_.headOption).map(_.partitions), Some(Vector(0, 1, 2, 3)))
    finally
      coordinator.close()
      deleteTree(directory)
  }

  test("consumer range assignor uses deterministic contiguous partition blocks") {
    val directory = Files.createTempDirectory("cascade-consumer-range-test")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"), scheduleExpiration = false)
    def join(memberId: String) = coordinator.consumerHeartbeat(
      ConsumerHeartbeatCommand(
        "range-workers", memberId, 0, None, None, 30_000,
        Some(Vector("events")), Some("range"), Some(Vector.empty)
      ),
      _ => 5
    )
    try
      val first = join("member-a")
      assertEquals(first.errorCode, Errors.None)
      val second = join("member-b")
      assertEquals(second.assignment.flatMap(_.headOption).map(_.partitions), Some(Vector(3, 4)))
      val advanced = coordinator.consumerHeartbeat(
        ConsumerHeartbeatCommand("range-workers", "member-a", first.memberEpoch, None, None, -1, None, None, None),
        _ => 5
      )
      assertEquals(advanced.assignment.flatMap(_.headOption).map(_.partitions), Some(Vector(0, 1, 2)))
    finally
      coordinator.close()
      deleteTree(directory)
  }

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

  test("a replacement static member fences every request from the previous owner") {
    val directory = Files.createTempDirectory("cascade-static-member-fencing")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"), scheduleExpiration = false)
    try
      val first = coordinator.join(command("", Array[Byte](1), Some("worker-a")))
      assertEquals(first.errorCode, Errors.None)
      assert(first.memberId.nonEmpty)
      assertEquals(
        coordinator.sync("workers", first.generationId, first.memberId, Some("worker-a"), Vector(first.memberId -> Array[Byte](9))).errorCode,
        Errors.None
      )

      val replacement = coordinator.join(command("", Array[Byte](2), Some("worker-a")))
      assertEquals(replacement.errorCode, Errors.None)
      assertNotEquals(replacement.memberId, first.memberId)
      assertEquals(coordinator.heartbeat("workers", first.generationId, first.memberId, Some("worker-a")), Errors.FencedInstanceId)
      assertEquals(
        coordinator.sync("workers", first.generationId, first.memberId, Some("worker-a"), Vector.empty).errorCode,
        Errors.FencedInstanceId
      )
      assertEquals(
        coordinator.commitOffsets("workers", first.generationId, first.memberId, Some("worker-a"), Vector.empty),
        Errors.FencedInstanceId
      )
    finally
      coordinator.close()
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

  private def command(
      memberId: String,
      metadata: Array[Byte],
      groupInstanceId: Option[String] = None
  ): JoinGroupCommand =
    JoinGroupCommand(
      groupId = "workers",
      sessionTimeoutMillis = 10_000,
      rebalanceTimeoutMillis = 10_000,
      memberId = memberId,
      groupInstanceId = groupInstanceId,
      protocolType = "consumer",
      protocols = Vector(GroupProtocol("range", metadata)),
      clientId = "coordinator-test"
    )

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

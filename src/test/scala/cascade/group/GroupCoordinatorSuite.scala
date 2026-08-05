package cascade.group

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

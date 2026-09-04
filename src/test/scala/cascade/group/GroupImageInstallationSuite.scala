package cascade.group

import java.nio.file.Files
import munit.FunSuite

final class GroupImageInstallationSuite extends FunSuite:
  private val classic = StoredGroup("classic", GroupStatus.Stable, 1, "a", "consumer", "range", 0L,
    Vector(StoredMember("a", None, 10000, 10000, Vector(StoredProtocol("range", Vector[Byte](1))), "client", 0L, Vector[Byte](2))),
    Vector.empty, Vector.empty)
  private val modern = StoredConsumerGroup("modern", 1, Vector(StoredConsumerMember("b", None, None, 10000,
    Vector("events"), "range", 1, 0L, Vector.empty)))
  private val initial = GroupImage(1L, Vector(classic), Vector.empty, Vector(modern))

  private def withCoordinator(body: GroupCoordinator => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-image-install-")
    val coordinator = GroupCoordinator(directory.resolve("offsets.log"), durableLocal = false, scheduleExpiration = false)
    try body(coordinator)
    finally
      coordinator.close()
      Files.deleteIfExists(directory.resolve("offsets.log"))
      Files.deleteIfExists(directory)

  test("ordinary committed images do not turn unrelated offset writes into member heartbeats") {
    withCoordinator { coordinator =>
      coordinator.installCommittedImage(initial, renewSessions = true)
      val before = coordinator.image
      Thread.sleep(20L) // Ensure a distinct wall-clock tick; assertions compare stored deadlines, not elapsed duration.
      val offset = OffsetCommitValue(GroupOffsetKey("unrelated", "events", 0), CommittedOffset(42L, -1, None, 1L))
      (2L to 10L).foreach(version => coordinator.installCommittedImage(initial.copy(version = version, offsets = Vector(offset)), renewSessions = false))
      val after = coordinator.image
      assertEquals(after.groups.head.members.head.lastHeartbeatMillis, before.groups.head.members.head.lastHeartbeatMillis)
      assertEquals(after.consumerGroups.head.members.head.lastHeartbeatMillis, before.consumerGroups.head.members.head.lastHeartbeatMillis)
      coordinator.expireNow(before.groups.head.members.head.lastHeartbeatMillis + 10000L)
      assert(coordinator.image.groups.head.members.isEmpty)
      coordinator.expireNow(before.consumerGroups.head.members.head.lastHeartbeatMillis + 45000L)
      assert(coordinator.image.consumerGroups.head.members.isEmpty)
      assertEquals(coordinator.fetchOffset(offset.key), Some(offset.value))
    }
  }

  test("recovery grants a new session window and removes authoritative deletions") {
    withCoordinator { coordinator =>
      coordinator.installCommittedImage(initial, renewSessions = true)
      val before = coordinator.image.groups.head.members.head.lastHeartbeatMillis
      Thread.sleep(20L)
      coordinator.installCommittedImage(initial, renewSessions = true)
      assert(coordinator.image.groups.head.members.head.lastHeartbeatMillis > before)
      coordinator.installCommittedImage(GroupImage.Empty, renewSessions = false)
      assertEquals(coordinator.image, GroupImage.Empty)
    }
  }

  test("installation retains newer local and replicated heartbeats without trusting an older snapshot") {
    withCoordinator { coordinator =>
      coordinator.installCommittedImage(initial, renewSessions = true)
      val before = coordinator.image
      val advanced = before.copy(groups = before.groups.map(g => g.copy(members = g.members.map(m => m.copy(lastHeartbeatMillis = m.lastHeartbeatMillis + 1000L)))))
      coordinator.installCommittedImage(advanced, renewSessions = false)
      coordinator.installCommittedImage(before, renewSessions = false)
      assertEquals(coordinator.image.groups.head.members.head.lastHeartbeatMillis, advanced.groups.head.members.head.lastHeartbeatMillis)
    }
  }

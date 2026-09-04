package cascade.group

import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit, TimeoutException}
import cascade.protocol.Errors
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
      Files.deleteIfExists(directory.resolve("offsets.log")): Unit
      Files.deleteIfExists(directory): Unit

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

  test("only the assigned ready owner expires members and readiness lag does not renew its lease") {
    withCoordinator { coordinator =>
      coordinator.installCommittedImage(initial, renewSessions = true)
      coordinator.expireOwned(100000L, _ == "classic", _ => true)
      coordinator.expireOwned(109000L, _ == "classic", _ => false)
      coordinator.expireOwned(110000L, _ == "classic", _ => true)
      assert(coordinator.image.groups.head.members.isEmpty)
      assertEquals(coordinator.image.consumerGroups.head.members.size, 1)
      coordinator.expireOwned(200000L, _ == "modern", _ => true)
      coordinator.expireOwned(244999L, _ == "modern", _ => true)
      assertEquals(coordinator.image.consumerGroups.head.members.size, 1)
      coordinator.expireOwned(245000L, _ == "modern", _ => true)
      assert(coordinator.image.consumerGroups.head.members.isEmpty)
    }
  }

  test("routing handoff grants grace once when a broker reacquires a group") {
    withCoordinator { coordinator =>
      coordinator.installCommittedImage(initial, renewSessions = true)
      coordinator.expireOwned(100000L, _ => true, _ => true)
      coordinator.expireOwned(105000L, _ => false, _ => true)
      coordinator.expireOwned(120000L, _ => true, _ => true)
      coordinator.expireOwned(129999L, _ => true, _ => true)
      assertEquals(coordinator.image.groups.head.members.size, 1)
      coordinator.expireOwned(130000L, _ => true, _ => true)
      assert(coordinator.image.groups.head.members.isEmpty)
    }
  }

  test("sync waiters observe installed assignments and authoritative group deletion") {
    Vector(false, true).foreach { deleted =>
      withCoordinator { coordinator =>
        val preparing = initial.copy(groups = Vector(classic.copy(status = GroupStatus.CompletingRebalance,
          rebalanceDeadlineMillis = System.currentTimeMillis() + 10000L)))
        coordinator.installCommittedImage(preparing, renewSessions = true)
        val executor = Executors.newSingleThreadExecutor()
        val started = CountDownLatch(1)
        try
          val result = executor.submit[SyncGroupResult](() =>
            started.countDown()
            coordinator.sync("classic", 1, "a", Vector.empty)
          )
          assert(started.await(1L, TimeUnit.SECONDS))
          intercept[TimeoutException](result.get(100L, TimeUnit.MILLISECONDS))
          val assignment = Vector[Byte](9, 8, 7)
          val installed = if deleted then GroupImage.Empty else initial.copy(groups = Vector(classic.copy(
            members = classic.members.map(_.copy(assignment = assignment)))))
          coordinator.installCommittedImage(installed, renewSessions = false)
          val synced = result.get(1L, TimeUnit.SECONDS)
          assertEquals(synced.errorCode, if deleted then Errors.UnknownMemberId else Errors.None)
          if !deleted then assertEquals(synced.assignment.toVector, assignment)
        finally
          executor.shutdownNow(): Unit
          executor.awaitTermination(2L, TimeUnit.SECONDS): Unit
      }
    }
  }

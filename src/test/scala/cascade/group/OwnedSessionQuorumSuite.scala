package cascade.group

import cascade.cluster.{ClusterNode, CoordinatorRouting}
import cascade.coordinator.*
import cascade.fault.FaultCluster
import cascade.protocol.{ApiKey, ByteCursor, ByteWriter, Errors}
import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import munit.FunSuite

final class OwnedSessionQuorumSuite extends FunSuite:
  test("a non-controller owner expires an idle group but preserves live heartbeats during unrelated quorum writes") {
    val cluster = FaultCluster(3, peerTimeoutMillis = 1500, heartbeatMillis = 250, electionTimeoutMillis = 5000)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val owner = cluster.nodes.find(_.id != controller.id).get
      val names = Iterator.from(0).map(i => s"owned-session-$i")
        .filter(key => CoordinatorRouting.owner(key, cluster.nodes).exists(_.id == owner.id)).take(2).toVector
      def group(name: String) = StoredGroup(name, GroupStatus.Stable, 1, "member", "consumer", "range", 0L,
        Vector(StoredMember("member", None, 1500, 10000, Vector(StoredProtocol("range", Vector.empty)), "client", 0L, Vector.empty)),
        Vector.empty, Vector.empty)
      val initial = CoordinatorProbe.snapshot(controller)._3.coordinator
      val seeded = GroupImage(1L, names.map(group), Vector.empty)
      val desired = GroupShardCodec.split(seeded) ++ initial.shardPayloads.drop(64)
      val seed = CoordinatorShardState.changes(initial, initial.shardPayloads, desired, CoordinatorProbe.snapshot(controller)._1).get
      assertEquals(CoordinatorProbe.commit(controller, seed), Errors.None)
      val end = System.nanoTime() + java.time.Duration.ofSeconds(6).toNanos
      var heartbeatSuccesses = 0
      var committedWrites = 0
      while System.nanoTime() < end do
        val heartbeat = sendHeartbeat(owner, names.head)
        assert(Set(Errors.None, Errors.NotCoordinator, Errors.CoordinatorNotAvailable)(heartbeat), s"live member failed: $heartbeat")
        if heartbeat == Errors.None then heartbeatSuccesses += 1
        val (term, _, metadata) = CoordinatorProbe.snapshot(controller)
        val current = metadata.coordinator
        val offset = OffsetCommitValue(GroupOffsetKey("unrelated-writes", "events", 0), CommittedOffset(committedWrites.toLong, -1, None, 1L))
        val updated = current.groupImage.copy(offsets = Vector(offset))
        val after = GroupShardCodec.split(updated) ++ current.shardPayloads.drop(64)
        CoordinatorShardState.changes(current, current.shardPayloads, after, term).foreach { delta =>
          if CoordinatorProbe.commit(controller, delta) == Errors.None then committedWrites += 1
        }
        Thread.sleep(50L)
      val stored = CoordinatorProbe.snapshot(controller)._3.coordinator.groupImage.groups.map(g => g.groupId -> g).toMap
      assert(heartbeatSuccesses >= 10, s"insufficient live traffic: $heartbeatSuccesses")
      assert(committedWrites >= 10, s"insufficient metadata churn: $committedWrites")
      assertEquals(stored(names.head).members.map(_.memberId), Vector("member"))
      assert(stored(names.last).members.isEmpty, "unrelated quorum writes kept an abandoned member alive")
    finally cluster.close()
  }

  private def sendHeartbeat(node: ClusterNode, group: String): Short =
    val socket = Socket(node.host, node.port)
    socket.setSoTimeout(3000)
    try
      val frame = ByteWriter().writeShort(ApiKey.Heartbeat).writeShort(3).writeInt(1).writeNullableString(Some("session-test"))
        .writeString(group).writeInt(1).writeString("member").writeNullableString(None).result()
      val output = DataOutputStream(socket.getOutputStream)
      output.writeInt(frame.length)
      output.write(frame)
      output.flush()
      val input = DataInputStream(socket.getInputStream)
      val length = input.readInt()
      require(length == 10, "invalid heartbeat response length")
      val cursor = ByteCursor(input.readNBytes(length))
      cursor.readInt()
      cursor.readInt()
      val code = cursor.readShort()
      cursor.ensureFullyRead()
      code
    finally socket.close()

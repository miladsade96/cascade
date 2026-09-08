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
        .filter(key => CoordinatorRouting.owner(CoordinatorKey.group(key).routingKey, cluster.nodes).exists(_.id == owner.id)).take(2).toVector
      val members = names.map(name => name -> joinAndSync(owner, name)).toMap
      val end = System.nanoTime() + java.time.Duration.ofSeconds(6).toNanos
      var heartbeatSuccesses = 0
      var committedWrites = 0
      while System.nanoTime() < end do
        val active = members(names.head)
        val heartbeat = sendHeartbeat(owner, names.head, active.generationId, active.memberId)
        assert(Set(Errors.None, Errors.NotCoordinator, Errors.CoordinatorNotAvailable)(heartbeat), s"live member failed: $heartbeat")
        if heartbeat == Errors.None then heartbeatSuccesses += 1
        val churnGroup = s"unrelated-writes-${committedWrites % 17}"
        val churnOwner = CoordinatorRouting.owner(CoordinatorKey.group(churnGroup).routingKey, cluster.nodes).get
        if sendOffsetCommit(churnOwner, churnGroup, committedWrites.toLong) == Errors.None then committedWrites += 1
        Thread.sleep(50L)
      val stored = CoordinatorProbe.snapshot(controller)._3.coordinator.groupImage.groups.map(g => g.groupId -> g).toMap
      assert(heartbeatSuccesses >= 10, s"insufficient live traffic: $heartbeatSuccesses")
      assert(committedWrites >= 10, s"insufficient metadata churn: $committedWrites")
      assertEquals(stored(names.head).members.map(_.memberId), Vector(members(names.head).memberId))
      assert(stored(names.last).members.isEmpty, "unrelated quorum writes kept an abandoned member alive")
    finally cluster.close()
  }

  private final case class Joined(generationId: Int, memberId: String)

  private def joinAndSync(node: ClusterNode, group: String): Joined =
    val first = sendJoin(node, group, "")
    assertEquals(first._1, Errors.MemberIdRequired)
    val joined = sendJoin(node, group, first._3)
    assertEquals(joined._1, Errors.None)
    val syncRequest = ByteWriter().writeShort(ApiKey.SyncGroup).writeShort(3).writeInt(3)
      .writeNullableString(Some("session-test")).writeString(group).writeInt(joined._2)
      .writeString(joined._3).writeNullableString(None)
    syncRequest.writeArray(Vector(joined._3)) { member =>
      syncRequest.writeString(member).writeByteArray(Array.emptyByteArray): Unit
    }
    val sync = request(node, syncRequest.result())
    sync.readInt()
    sync.readInt()
    val syncError = sync.readShort()
    sync.readByteArray()
    sync.ensureFullyRead()
    assertEquals(syncError, Errors.None)
    Joined(joined._2, joined._3)

  private def sendJoin(node: ClusterNode, group: String, memberId: String): (Short, Int, String) =
    val joinRequest = ByteWriter().writeShort(ApiKey.JoinGroup).writeShort(5).writeInt(2)
      .writeNullableString(Some("session-test")).writeString(group).writeInt(1500).writeInt(10000)
      .writeString(memberId).writeNullableString(None).writeString("consumer")
    joinRequest.writeArray(Vector("range")) { protocol =>
      joinRequest.writeString(protocol).writeByteArray(Array.emptyByteArray): Unit
    }
    val cursor = request(node, joinRequest.result())
    cursor.readInt()
    cursor.readInt()
    val error = cursor.readShort()
    val generation = cursor.readInt()
    cursor.readString()
    cursor.readString()
    val assigned = cursor.readString()
    cursor.readArray {
      cursor.readString()
      cursor.readNullableString()
      cursor.readByteArray()
    }
    cursor.ensureFullyRead()
    (error, generation, assigned)

  private def sendHeartbeat(node: ClusterNode, group: String, generationId: Int, memberId: String): Short =
    val cursor = request(node,
      ByteWriter().writeShort(ApiKey.Heartbeat).writeShort(3).writeInt(1).writeNullableString(Some("session-test"))
        .writeString(group).writeInt(generationId).writeString(memberId).writeNullableString(None).result()
    )
    cursor.readInt()
    cursor.readInt()
    val code = cursor.readShort()
    cursor.ensureFullyRead()
    code

  private def sendOffsetCommit(node: ClusterNode, group: String, offset: Long): Short =
    val commitRequest = ByteWriter().writeShort(ApiKey.OffsetCommit).writeShort(7).writeInt(4)
      .writeNullableString(Some("session-test")).writeString(group).writeInt(-1).writeString("")
      .writeNullableString(None)
    commitRequest.writeArray(Vector("coordinator-qualification")) { topic =>
      commitRequest.writeString(topic).writeArray(Vector(0)) { partition =>
        commitRequest.writeInt(partition).writeLong(offset).writeInt(-1).writeNullableString(None): Unit
      }: Unit
    }
    val cursor = request(node, commitRequest.result())
    cursor.readInt()
    cursor.readInt()
    val responses = cursor.readArray {
      cursor.readString()
      cursor.readArray {
        cursor.readInt()
        cursor.readShort()
      }
    }
    cursor.ensureFullyRead()
    responses.head.head

  private def request(node: ClusterNode, frame: Array[Byte]): ByteCursor =
    val socket = Socket(node.host, node.port)
    socket.setSoTimeout(3000)
    try
      val output = DataOutputStream(socket.getOutputStream)
      output.writeInt(frame.length)
      output.write(frame)
      output.flush()
      val input = DataInputStream(socket.getInputStream)
      val length = input.readInt()
      ByteCursor(input.readNBytes(length))
    finally socket.close()

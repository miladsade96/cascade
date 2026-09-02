package cascade.cluster

import cascade.coordinator.CoordinatorProbe
import cascade.fault.FaultCluster
import cascade.protocol.{ByteCursor, Errors}
import munit.FunSuite

final class MetadataDeltaReplicationSuite extends FunSuite:
  private def send(node: ClusterNode, payload: Array[Byte]): Short =
    val peer = PeerClient()
    try
      val response = peer.call(node, InternalApi.MetadataDeltaCommit, payload, 3000)
      response.readLong()
      val error = response.readShort()
      response.ensureFullyRead()
      error
    finally peer.close()

  test("a live quorum replicates compact deltas and idempotently acknowledges retransmission") {
    val cluster = FaultCluster(3, peerTimeoutMillis = 1500, heartbeatMillis = 250, electionTimeoutMillis = 5000)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      (1 to 40).foreach { offset =>
        val base = CoordinatorProbe.snapshot(controller)._3
        val (delta, _) = MetadataDeltaFixture.update(base, s"wire-group-$offset", offset.toLong)
        assertEquals(CoordinatorProbe.commit(controller, delta.change), Errors.None)
      }
      val committed = CoordinatorProbe.snapshot(controller)._3
      val calls = cluster.faults.calls.filter(_.apiKey == InternalApi.MetadataDeltaCommit)
      assert(calls.size >= 80, s"expected replicated deltas on both follower links, got ${calls.size}")
      val last = calls.last
      assert(last.payload.size < MetadataCodec.encode(committed).length / 2)
      assertEquals(send(cluster.nodes(last.targetId - 1), last.payload.toArray), Errors.None)
      assertEquals(CoordinatorProbe.snapshot(cluster.nodes(last.targetId - 1))._3.coordinator, committed.coordinator)
      val journalMetrics = cluster.nodes.map(n => cluster.broker(n.id).metricsSnapshot.metadataJournal)
      assert(journalMetrics.forall(_.deltaRecords >= 40L))
      assert(journalMetrics.forall(_.deltaBytes > 0L))
      val cursor = ByteCursor(last.payload.toArray)
      val term = cursor.readLong()
      val leader = cursor.readInt()
      val delta = MetadataDeltaCodec.decode(cursor.readByteArray())
      val stale = cascade.protocol.ByteWriter().writeLong(term).writeInt(leader)
        .writeByteArray(MetadataDeltaCodec.encode(delta.copy(baseVersion = delta.baseVersion + 100L))).result()
      assertEquals(send(cluster.nodes(last.targetId - 1), stale), Errors.InvalidRequest)
      assertEquals(CoordinatorProbe.snapshot(cluster.nodes(last.targetId - 1))._3.coordinator, committed.coordinator)
    finally cluster.close()
  }

  test("all brokers recover exact incremental coordinator state after a full restart") {
    val cluster = FaultCluster(3, peerTimeoutMillis = 1500, heartbeatMillis = 250, electionTimeoutMillis = 5000)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      (1 to 12).foreach { offset =>
        val base = CoordinatorProbe.snapshot(controller)._3
        assertEquals(CoordinatorProbe.commit(controller, MetadataDeltaFixture.update(base, s"replay-$offset", offset)._1.change), Errors.None)
      }
      val committed = CoordinatorProbe.snapshot(controller)._3.coordinator
      cluster.nodes.foreach(n => cluster.stop(n.id))
      cluster.startAll()
      val recoveredController = CoordinatorProbe.controller(cluster.nodes)
      val recovered = CoordinatorProbe.snapshot(recoveredController)._3.coordinator
      assertEquals(recovered.groupState, committed.groupState)
      assertEquals(recovered.deliveryState, committed.deliveryState)
      assertEquals(recovered.shardVersions, committed.shardVersions)
    finally cluster.close()
  }

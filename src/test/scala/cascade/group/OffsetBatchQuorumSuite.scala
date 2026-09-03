package cascade.group

import cascade.cluster.{ClusterNode, CoordinatorRouting, InternalApi}
import cascade.coordinator.CoordinatorProbe
import cascade.fault.{FaultCluster, FaultSelector}
import cascade.protocol.{ApiKey, ByteCursor, ByteWriter, Errors}
import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import munit.FunSuite

final class OffsetBatchQuorumSuite extends FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(60L, "seconds")

  test("batched wire commits fail together without a quorum and recover exactly after retry and restart") {
    val cluster = FaultCluster(3, peerTimeoutMillis = 1500, heartbeatMillis = 250, electionTimeoutMillis = 10000,
      offsetBatch = OffsetBatchConfig(maxRequests = 8, lingerMillis = 100L))
    val executor = Executors.newFixedThreadPool(8)
    try
      cluster.startAll()
      CoordinatorProbe.activate(cluster.bootstrapServers)
      val controller = CoordinatorProbe.controller(cluster.nodes)
      val groups = Iterator.from(0).map(i => s"batch-quorum-$i")
        .filter(group => CoordinatorRouting.owner(group, cluster.nodes).exists(_.id == controller.id)).take(8).toVector
      def parallel(value: Long): Vector[Short] =
        val start = CountDownLatch(1)
        val results = groups.map(group => executor.submit[Short](() => { start.await(); commit(controller, group, value) }))
        start.countDown()
        results.map(_.get(10L, TimeUnit.SECONDS))
      def offsets: Map[String, Long] = GroupCodec.decode(CoordinatorProbe.snapshot(controller)._3.coordinator.groupState.toArray)
        .offsets.map(v => v.key.groupId -> v.value.offset).toMap
      assertEquals(parallel(1L), Vector.fill(8)(Errors.None))
      val baseline = offsets
      cluster.nodes.filterNot(_.id == controller.id).foreach { node =>
        cluster.faults.block(FaultSelector(controller.id, node.id, Some(InternalApi.MetadataDeltaCommit)))
        cluster.faults.block(FaultSelector(controller.id, node.id, Some(InternalApi.MetadataCommit)))
      }
      val failed = parallel(2L)
      assert(failed.forall(_ != Errors.None), s"unexpected acknowledgements: $failed")
      assertEquals(offsets, baseline)
      assert(cluster.broker(controller.id).metricsSnapshot.offsetBatch.failed >= 8L)
      cluster.faults.heal()
      assertEquals(parallel(3L), Vector.fill(8)(Errors.None))
      val measured = cluster.broker(controller.id).metricsSnapshot.offsetBatch
      assert(measured.batchRequests > measured.batches, s"no requests coalesced: $measured")
      assertEquals(measured.rejected, 0L)
      assertEquals(offsets, groups.map(_ -> 3L).toMap)
      cluster.nodes.foreach(node => cluster.stop(node.id))
      cluster.startAll()
      val recovered = CoordinatorProbe.controller(cluster.nodes)
      val recoveredOffsets = GroupCodec.decode(CoordinatorProbe.snapshot(recovered)._3.coordinator.groupState.toArray)
        .offsets.map(v => v.key.groupId -> v.value.offset).toMap
      assertEquals(recoveredOffsets, groups.map(_ -> 3L).toMap)
    finally
      cluster.faults.heal()
      executor.close()
      cluster.close()
  }

  private def commit(node: ClusterNode, group: String, offset: Long): Short =
    val socket = Socket(node.host, node.port)
    socket.setSoTimeout(10000)
    try
      val frame = ByteWriter().writeShort(ApiKey.OffsetCommit).writeShort(5).writeInt(1).writeNullableString(Some("batch-test"))
        .writeString(group).writeInt(-1).writeString("").writeInt(1).writeString("coordinator-qualification")
        .writeInt(1).writeInt(0).writeLong(offset).writeNullableString(None).result()
      val output = DataOutputStream(socket.getOutputStream)
      output.writeInt(frame.length)
      output.write(frame)
      output.flush()
      val input = DataInputStream(socket.getInputStream)
      val length = input.readInt()
      require(length > 0 && length <= 4096, "invalid response size")
      val response = ByteCursor(input.readNBytes(length))
      response.readInt()
      response.readInt()
      val errors = response.readArray {
        response.readString()
        response.readArray { response.readInt(); response.readShort() }
      }.flatten
      response.ensureFullyRead()
      require(errors.size == 1, "unexpected partition count")
      errors.head
    finally socket.close()

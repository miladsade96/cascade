package cascade.group

import cascade.cluster.{ClusterNode, CoordinatorRouting, InternalApi, MetadataStore, ShardStorageFixture}
import cascade.backup.BackupRestore
import cascade.coordinator.CoordinatorProbe
import cascade.fault.{FaultCluster, FaultSelector}
import cascade.protocol.{ApiKey, ByteCursor, ByteWriter, Errors}
import java.io.{DataInputStream, DataOutputStream}
import java.net.Socket
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit, TimeoutException}
import munit.FunSuite

final class OffsetBatchQuorumSuite extends FunSuite:
  override val munitTimeout = scala.concurrent.duration.Duration(60L, "seconds")

  test("online snapshots wait for batched publication and restore its acknowledged offsets") {
    ShardStorageFixture.withDirectory { root =>
      val cluster = FaultCluster(3, peerTimeoutMillis = 3000, heartbeatMillis = 250, electionTimeoutMillis = 10000)
      val executor = Executors.newFixedThreadPool(2)
      val persisted = CountDownLatch(2)
      val release = CountDownLatch(1)
      val snapshotStarted = CountDownLatch(1)
      try
        cluster.startAll()
        CoordinatorProbe.activate(cluster.bootstrapServers)
        val controller = CoordinatorProbe.controller(cluster.nodes)
        val group = Iterator.from(0).map(i => s"batch-snapshot-$i")
          .find(key => CoordinatorRouting.owner(key, cluster.nodes).exists(_.id == controller.id)).get
        cluster.faults.observeReplies { (call, _) =>
          if call.sourceId == controller.id && call.apiKey == InternalApi.MetadataDeltaCommit then
            persisted.countDown()
            if !release.await(3L, TimeUnit.SECONDS) then throw IllegalStateException("snapshot publication barrier timed out")
        }
        val write = executor.submit[Short](() => commit(controller, group, 42L))
        assert(persisted.await(3L, TimeUnit.SECONDS))
        val snapshot = executor.submit[cascade.backup.BackupManifest](() =>
          snapshotStarted.countDown()
          cluster.broker(controller.id).createOnlineSnapshot(root.resolve("backup")))
        assert(snapshotStarted.await(1L, TimeUnit.SECONDS))
        intercept[TimeoutException](snapshot.get(50L, TimeUnit.MILLISECONDS))
        release.countDown()
        assertEquals(write.get(5L, TimeUnit.SECONDS), Errors.None)
        val manifest = snapshot.get(5L, TimeUnit.SECONDS)
        assert(manifest.entries.exists(_.relativePath.contains(".shards/")))
        BackupRestore.restore(root.resolve("backup"), root.resolve("restored"))
        val recovered = MetadataStore(root.resolve("restored/.cascade/cluster-metadata.log"))
        try
          val offsets = GroupCodec.decode(recovered.metadata.coordinator.groupState.toArray).offsets
          assertEquals(offsets.map(v => v.key.groupId -> v.value.offset), Vector(group -> 42L))
        finally recovered.close()
      finally
        release.countDown()
        cluster.faults.heal()
        executor.close()
        cluster.close()
    }
  }

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

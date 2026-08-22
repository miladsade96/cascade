package cascade.storage

import cascade.TestRecordBatch
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files
import java.nio.file.StandardOpenOption
import java.nio.file.attribute.FileTime
import java.util.concurrent.atomic.AtomicInteger
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class PartitionLogSuite extends FunSuite:
  test("assigns monotonic offsets, rolls segments, and recovers its index") {
    val directory = Files.createTempDirectory("cascade-log-test")
    try
      val log = PartitionLog(directory, maxSegmentBytes = 1024)
      try
        (0 until 20).foreach { expected =>
          val result = log.append(TestRecordBatch.single())
          assertEquals(result.baseOffset, expected.toLong)
        }
        assertEquals(log.highWatermark, 20L)
        assertEquals(log.fetch(5L, 122).records.length, 122)
      finally log.close()

      val recovered = PartitionLog(directory, maxSegmentBytes = 1024)
      try
        assertEquals(recovered.highWatermark, 20L)
        val fetched = recovered.fetch(19L, 1024)
        assertEquals(fetched.records.length, 61)
        assertEquals(RecordBatch.baseOffset(fetched.records), 19L)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("fetch pages stop before the first batch that exceeds the byte limit") {
    val directory = Files.createTempDirectory("cascade-fetch-page-test")
    try
      val log = PartitionLog(directory, maxSegmentBytes = 1024)
      try
        log.append(TestRecordBatch.single(totalBytes = 100))
        log.append(TestRecordBatch.single(totalBytes = 100))
        log.append(TestRecordBatch.single(totalBytes = 61))

        val firstPage = log.fetch(0L, 161).records
        val secondPage = log.fetch(1L, 161).records

        assertEquals(firstPage.length, 100)
        assertEquals(secondPage.length, 161)
        assertEquals(batchBaseOffsets(firstPage) ++ batchBaseOffsets(secondPage), Vector(0L, 1L, 2L))
      finally log.close()
    finally deleteTree(directory)
  }

  test("periodic policy batches forces by threshold instead of forcing each append") {
    val directory = Files.createTempDirectory("cascade-periodic-flush-test")
    val flushRequests = AtomicInteger(0)
    try
      val log = PartitionLog(
        directory,
        maxSegmentBytes = 1024 * 1024,
        flushPolicy = FlushPolicy.Periodic,
        flushIntervalMillis = 60_000,
        maxUnflushedBytes = 122,
        requestFlush = () => flushRequests.incrementAndGet(): Unit
      )
      try
        log.append(TestRecordBatch.single())
        assertEquals(log.flushStatistics, FlushStatistics(0L, 0L, 0L, 61L))

        log.append(TestRecordBatch.single())
        assertEquals(flushRequests.get(), 1)
        assert(log.flushIfNeeded())
        val statistics = log.flushStatistics
        assertEquals(statistics.forces, 1L)
        assertEquals(statistics.bytes, 122L)
        assertEquals(statistics.pendingBytes, 0L)
      finally log.close()
    finally deleteTree(directory)
  }

  test("sync policy forces every append") {
    val directory = Files.createTempDirectory("cascade-sync-flush-test")
    try
      val log = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        log.append(TestRecordBatch.single())
        val statistics = log.flushStatistics
        assertEquals(statistics.forces, 1L)
        assertEquals(statistics.bytes, 61L)
        assertEquals(statistics.pendingBytes, 0L)
      finally log.close()
    finally deleteTree(directory)
  }

  test("replica appends remain invisible until their high watermark is committed") {
    val directory = Files.createTempDirectory("cascade-replica-watermark-test")
    try
      val log = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        val result = log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 0L)
        assertEquals(result.baseOffset, 0L)
        assertEquals(log.logEndOffset, 1L)
        assertEquals(log.highWatermark, 0L)
        assertEquals(log.fetch(0L, 1024).records.length, 0)

        log.commitThrough(1L)
        assertEquals(log.highWatermark, 1L)
        assertEquals(log.fetch(0L, 1024).records.length, 61)
        intercept[cascade.protocol.ProtocolException](log.appendReplica(TestRecordBatch.single(), 0L))
      finally log.close()
    finally deleteTree(directory)
  }

  test("replica high watermark survives restart without exposing an uncommitted tail") {
    val directory = Files.createTempDirectory("cascade-replica-watermark-restart-test")
    try
      val log = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 0L)
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 1L)
        log.commitThrough(1L)
        assertEquals(log.logEndOffset, 2L)
        assertEquals(log.highWatermark, 1L)
      finally log.close()

      val recovered = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        assertEquals(recovered.logEndOffset, 2L)
        assertEquals(recovered.highWatermark, 1L)
        assertEquals(batchBaseOffsets(recovered.fetch(0L, 1024).records), Vector(0L))
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("a high watermark ahead of a repaired log is clamped and re-checkpointed") {
    val directory = Files.createTempDirectory("cascade-replica-watermark-clamp-test")
    val segment = directory.resolve("00000000000000000000.log")
    try
      val log = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 0L)
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 1L)
        log.commitThrough(2L)
      finally log.close()

      Files.write(segment, TestRecordBatch.single().take(20), StandardOpenOption.APPEND)
      val channel = java.nio.channels.FileChannel.open(segment, StandardOpenOption.WRITE)
      try channel.truncate(61L)
      finally channel.close()

      val recovered = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        assertEquals(recovered.logEndOffset, 1L)
        assertEquals(recovered.highWatermark, 1L)
      finally recovered.close()

      val reopened = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try assertEquals(reopened.highWatermark, 1L)
      finally reopened.close()
    finally deleteTree(directory)
  }

  test("an existing checkpoint with no valid slot fails closed at the log start") {
    val directory = Files.createTempDirectory("cascade-replica-watermark-corrupt-test")
    val checkpoint = directory.resolve("high-watermark.checkpoint")
    try
      val log = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 0L)
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 1L)
        log.commitThrough(2L)
      finally log.close()

      Files.write(checkpoint, Array.fill[Byte](64)(0x55.toByte), StandardOpenOption.TRUNCATE_EXISTING)
      val recovered = PartitionLog(directory, flushPolicy = FlushPolicy.Sync)
      try
        assertEquals(recovered.logEndOffset, 2L)
        assertEquals(recovered.highWatermark, 0L)
        assertEquals(recovered.fetch(0L, 1024).records.length, 0)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("replica reset removes a divergent tail and rebuilds from an authoritative offset") {
    val directory = Files.createTempDirectory("cascade-replica-reset-test")
    try
      val log = PartitionLog(directory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 0L)
        log.appendReplica(TestRecordBatch.single(), expectedBaseOffset = 1L)
        log.commitThrough(1L)
        assertEquals(log.logEndOffset, 2L)
        assertEquals(log.highWatermark, 1L)

        log.resetReplica(0L)
        assertEquals(log.logStartOffset, 0L)
        assertEquals(log.logEndOffset, 0L)
        assertEquals(log.highWatermark, 0L)
        assertEquals(log.fetch(0L, 1024).records.length, 0)

        log.appendReplica(TestRecordBatch.single(totalBytes = 100), expectedBaseOffset = 0L)
        log.commitThrough(1L)
        assertEquals(log.fetch(0L, 1024).records.length, 100)
        val paths = Files.list(directory)
        try assertEquals(paths.iterator().asScala.count(_.getFileName.toString.endsWith(".log")), 1)
        finally paths.close()
      finally log.close()
    finally deleteTree(directory)
  }

  test("recovery fingerprints identify a shared prefix and change after divergence") {
    val leaderDirectory = Files.createTempDirectory("cascade-recovery-fingerprint-leader")
    val followerDirectory = Files.createTempDirectory("cascade-recovery-fingerprint-follower")
    try
      val leader = PartitionLog(leaderDirectory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      val follower = PartitionLog(followerDirectory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try
        (0 until 5).foreach { index =>
          val bytes = TestRecordBatch.single(totalBytes = 61 + index)
          leader.appendReplica(bytes, index.toLong)
          follower.appendReplica(bytes, index.toLong)
        }
        leader.appendReplica(TestRecordBatch.single(totalBytes = 100), 5L)
        follower.appendReplica(TestRecordBatch.single(totalBytes = 101), 5L)

        assertEquals(leader.recoveryFingerprint(4L), follower.recoveryFingerprint(4L))
        assertNotEquals(leader.recoveryFingerprint(5L), follower.recoveryFingerprint(5L))
        assertEquals(leader.recoveryProbe(4L, 6L).map(_.baseOffset), Some(4L))
      finally
        follower.close()
        leader.close()
    finally
      deleteTree(followerDirectory)
      deleteTree(leaderDirectory)
  }

  test("replica truncation preserves a verified prefix and deletes divergent segments") {
    val directory = Files.createTempDirectory("cascade-replica-truncate-test")
    try
      val log = PartitionLog(directory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try
        (0 until 20).foreach(index => log.appendReplica(TestRecordBatch.single(totalBytes = 100), index.toLong))
        log.commitThrough(10L)
        assert(log.recoveryFingerprint(19L).nonEmpty)

        log.truncateReplicaTo(10L)
        assertEquals(log.logEndOffset, 10L)
        assertEquals(log.highWatermark, 10L)
        assertEquals(log.recoverySummary(0L, 10L, 20).map(_.baseOffset), (0L until 10L).toVector)
        assertEquals(log.appendReplica(TestRecordBatch.single(), 10L).baseOffset, 10L)
      finally log.close()

      val recovered = PartitionLog(directory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try
        assertEquals(recovered.logEndOffset, 11L)
        assertEquals(recovered.highWatermark, 10L)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("recovery truncates an incomplete batch from the active segment") {
    val directory = Files.createTempDirectory("cascade-tail-recovery-test")
    try
      val log = PartitionLog(directory)
      try log.append(TestRecordBatch.single())
      finally log.close()

      val segment = directory.resolve("00000000000000000000.log")
      Files.write(segment, TestRecordBatch.single().take(20), StandardOpenOption.APPEND)
      assertEquals(Files.size(segment), 81L)

      val recovered = PartitionLog(directory)
      try
        assertEquals(recovered.highWatermark, 1L)
        assertEquals(Files.size(segment), 61L)
        assertEquals(recovered.append(TestRecordBatch.single()).baseOffset, 1L)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("recovery discards segments after an incomplete earlier tail") {
    val directory = Files.createTempDirectory("cascade-multisegment-recovery-test")
    try
      val log = PartitionLog(directory, maxSegmentBytes = 1024)
      try (0 until 17).foreach(_ => log.append(TestRecordBatch.single()))
      finally log.close()

      val firstSegment = directory.resolve("00000000000000000000.log")
      val laterSegment = directory.resolve("00000000000000000016.log")
      assert(Files.exists(laterSegment))
      Files.write(firstSegment, TestRecordBatch.single().take(20), StandardOpenOption.APPEND)

      val recovered = PartitionLog(directory, maxSegmentBytes = 1024)
      try
        assertEquals(recovered.highWatermark, 16L)
        assert(!Files.exists(laterSegment))
        assertEquals(recovered.append(TestRecordBatch.single()).baseOffset, 16L)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("topic registry flushes dirty partitions in the background") {
    val directory = Files.createTempDirectory("cascade-background-flush-test")
    try
      val registry = TopicRegistry(
        directory,
        maxSegmentBytes = 1024 * 1024,
        flushPolicy = FlushPolicy.Periodic,
        flushIntervalMillis = 20,
        flushBytes = Long.MaxValue
      )
      try
        assertEquals(registry.createTopic("events", 1), CreateTopicResult.Created)
        val log = registry.partition("events", 0).getOrElse(fail("missing partition"))
        log.append(TestRecordBatch.single())
        val deadline = System.nanoTime() + 2_000_000_000L
        while registry.flushStatistics.forces == 0L && System.nanoTime() < deadline do Thread.sleep(5)
        assertEquals(registry.flushStatistics.forces, 1L)
        assertEquals(registry.flushStatistics.pendingBytes, 0L)
      finally registry.close()
    finally deleteTree(directory)
  }

  test("timestamp and transaction indexes rebuild from immutable batches after restart") {
    val directory = Files.createTempDirectory("cascade-lifecycle-index-test")
    try
      val log = PartitionLog(directory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try
        log.append(TestRecordBatch.keyed(Vector(TestRecordBatch.Record(Some("a".getBytes), Some("1".getBytes), 1000L))))
        log.append(TestRecordBatch.keyed(Vector(TestRecordBatch.Record(Some("b".getBytes), Some("2".getBytes), 2000L))))
        log.append(TestRecordBatch.producer(20L, 1, 0, transactional = true))
        assertEquals(log.offsetForTimestamp(1500L), 1L)
        assertEquals(log.offsetForTimestamp(3000L), -1L)
        assertEquals(log.transactionBatches(0L, 3L).map(_.producerId), Vector(20L))
      finally log.close()

      val recovered = PartitionLog(directory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try
        assertEquals(recovered.offsetForTimestamp(1500L), 1L)
        assertEquals(recovered.transactionBatches(2L, 3L).map(_.producerId), Vector(20L))
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("disk-pressure admission rejects before changing the log") {
    val directory = Files.createTempDirectory("cascade-disk-pressure-test")
    try
      val log = PartitionLog(
        directory,
        lifecycleConfig = StorageLifecycleConfig(minimumFreeBytes = 100L),
        usableSpace = Some(() => 160L)
      )
      try
        intercept[StoragePressureException](log.append(TestRecordBatch.single()))
        assertEquals(log.logEndOffset, 0L)
        assertEquals(log.lifecycleStatistics.rejectedAppends, 1L)
      finally log.close()
    finally deleteTree(directory)
  }

  test("time retention atomically retires only closed committed segments") {
    val directory = Files.createTempDirectory("cascade-time-retention-test")
    try
      val log = PartitionLog(
        directory,
        maxSegmentBytes = 1024,
        flushPolicy = FlushPolicy.Sync,
        lifecycleConfig = StorageLifecycleConfig(retentionMillis = 1000L)
      )
      try
        log.append(TestRecordBatch.single(totalBytes = 600))
        log.append(TestRecordBatch.single(totalBytes = 600))
        val first = directory.resolve("00000000000000000000.log")
        Files.setLastModifiedTime(first, FileTime.fromMillis(1000L))

        val statistics = log.runLifecycle(nowMillis = 3000L)
        assertEquals(statistics.retiredSegments, 1L)
        assertEquals(statistics.reclaimedBytes, 600L)
        assertEquals(log.logStartOffset, 1L)
        assertEquals(batchBaseOffsets(log.fetch(0L, 4096).records), Vector(1L))
        assert(!Files.exists(first))
      finally log.close()

      val recovered = PartitionLog(directory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try
        assertEquals(recovered.logStartOffset, 1L)
        assertEquals(recovered.logEndOffset, 2L)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("size retention removes oldest closed segments until the partition reaches its budget") {
    val directory = Files.createTempDirectory("cascade-size-retention-test")
    try
      val log = PartitionLog(
        directory,
        maxSegmentBytes = 1024,
        flushPolicy = FlushPolicy.Sync,
        lifecycleConfig = StorageLifecycleConfig(retentionMillis = -1L, retentionBytes = 1000L)
      )
      try
        (0 until 3).foreach(_ => log.append(TestRecordBatch.single(totalBytes = 600)))
        val statistics = log.runLifecycle()
        assertEquals(statistics.retiredSegments, 2L)
        assertEquals(statistics.reclaimedBytes, 1200L)
        assertEquals(log.logStartOffset, 2L)
        assertEquals(log.logEndOffset, 3L)
        assertEquals(batchBaseOffsets(log.fetch(0L, 4096).records), Vector(2L))
      finally log.close()
    finally deleteTree(directory)
  }

  test("topic registry runs lifecycle maintenance for synchronous logs") {
    val directory = Files.createTempDirectory("cascade-lifecycle-scheduler-test")
    try
      val registry = TopicRegistry(
        directory,
        maxSegmentBytes = 1024,
        flushPolicy = FlushPolicy.Sync,
        lifecycleConfig = StorageLifecycleConfig(retentionMillis = 50L, lifecycleIntervalMillis = 20L)
      )
      try
        assertEquals(registry.createTopic("events", 1), CreateTopicResult.Created)
        val log = registry.partition("events", 0).getOrElse(fail("missing partition"))
        log.append(TestRecordBatch.single(totalBytes = 600))
        log.append(TestRecordBatch.single(totalBytes = 600))
        Files.setLastModifiedTime(
          directory.resolve("events").resolve("partition-0").resolve("00000000000000000000.log"),
          FileTime.fromMillis(1L)
        )

        val deadline = System.nanoTime() + 2_000_000_000L
        while registry.lifecycleStatistics.retiredSegments == 0L && System.nanoTime() < deadline do Thread.sleep(5L)
        assertEquals(registry.lifecycleStatistics.retiredSegments, 1L)
        assertEquals(log.logStartOffset, 1L)
      finally registry.close()
    finally deleteTree(directory)
  }

  test("key compaction rewrites closed segments and preserves latest, keyless, and transactional batches") {
    val directory = Files.createTempDirectory("cascade-key-compaction-test")
    val value = Array.fill[Byte](300)(7)
    try
      val log = PartitionLog(
        directory,
        maxSegmentBytes = 1024,
        flushPolicy = FlushPolicy.Sync,
        lifecycleConfig = StorageLifecycleConfig(
          cleanupPolicy = CleanupPolicy.Compact,
          retentionMillis = -1L
        )
      )
      try
        log.append(TestRecordBatch.keyed(Vector(TestRecordBatch.Record(Some("key".getBytes), Some(value), 1000L))))
        log.append(TestRecordBatch.keyed(Vector(TestRecordBatch.Record(None, Some(value), 1001L))))
        log.append(TestRecordBatch.producer(20L, 1, 0, transactional = true))
        log.append(TestRecordBatch.keyed(Vector(TestRecordBatch.Record(Some("key".getBytes), Some(value), 1002L))))

        val statistics = log.runLifecycle()
        assertEquals(statistics.compactedBatches, 1L)
        assert(statistics.reclaimedBytes > 300L)
        assertEquals(batchBaseOffsets(log.fetch(0L, 4096).records), Vector(1L, 2L, 3L))
        assertEquals(log.logStartOffset, 1L)
        assertEquals(log.logEndOffset, 4L)
        assertEquals(log.transactionBatches(0L, 4L).map(_.producerId), Vector(20L))
      finally log.close()

      val recovered = PartitionLog(directory, maxSegmentBytes = 1024, flushPolicy = FlushPolicy.Sync)
      try assertEquals(batchBaseOffsets(recovered.fetch(0L, 4096).records), Vector(1L, 2L, 3L))
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def batchBaseOffsets(records: Array[Byte]): Vector[Long] =
    val buffer = ByteBuffer.wrap(records).order(ByteOrder.BIG_ENDIAN)
    val offsets = Vector.newBuilder[Long]
    var position = 0
    while position < records.length do
      offsets += buffer.getLong(position)
      position += Math.addExact(buffer.getInt(position + 8), 12)
    offsets.result()

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

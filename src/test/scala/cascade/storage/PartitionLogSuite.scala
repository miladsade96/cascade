package cascade.storage

import cascade.TestRecordBatch
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.file.Files
import java.nio.file.StandardOpenOption
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

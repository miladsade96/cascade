package cascade.storage

import cascade.protocol.ProtocolException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.util.zip.CRC32C
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class ProducerStateCheckpointSuite extends FunSuite:
  test("round-trips bounded producer identity, epoch, sequence, and transaction flags") {
    val directory = Files.createTempDirectory("cascade-producer-state-checkpoint-test")
    val path = directory.resolve("producer-state.checkpoint")
    try
      val checkpoint = ProducerStateCheckpoint(path, historyLimit = 2)
      val retained = checkpoint.persist(Vector(
        batch(0L, 9L, 1, 0, transactional = false),
        batch(1L, 9L, 1, 1, transactional = true),
        batch(2L, 9L, 1, 2, transactional = true),
        batch(3L, 4L, 3, 10, transactional = false, control = true)
      ))

      assertEquals(retained.map(value => (value.producerId, value.baseOffset)), Vector(4L -> 3L, 9L -> 1L, 9L -> 2L))
      assertEquals(ProducerStateCheckpoint(path, historyLimit = 2).recovered, retained)
      assertEquals(retained.last.producerEpoch, 1.toShort)
      assertEquals(retained.last.lastSequence, 2)
      assert(retained(1).transactional)
      assert(retained.head.control)
    finally deleteTree(directory)
  }

  test("rejects a CRC-damaged producer state checkpoint") {
    val directory = Files.createTempDirectory("cascade-producer-state-corrupt-test")
    val path = directory.resolve("producer-state.checkpoint")
    try
      ProducerStateCheckpoint(path, historyLimit = 5).persist(Vector(batch(0L, 2L, 4, 7)))
      val damaged = Files.readAllBytes(path)
      damaged(20) = (damaged(20) ^ 0x01).toByte
      Files.write(path, damaged)

      val error = intercept[ProtocolException](ProducerStateCheckpoint(path, historyLimit = 5))
      assert(error.getMessage.contains("CRC32C mismatch"))
    finally deleteTree(directory)
  }

  test("atomically replaces an older checkpoint image") {
    val directory = Files.createTempDirectory("cascade-producer-state-replace-test")
    val path = directory.resolve("producer-state.checkpoint")
    try
      val checkpoint = ProducerStateCheckpoint(path, historyLimit = 5)
      checkpoint.persist(Vector(batch(0L, 1L, 2, 0)))
      val expected = checkpoint.persist(Vector(batch(5L, 1L, 3, 9), batch(6L, 2L, 1, 4)))

      assertEquals(ProducerStateCheckpoint(path, historyLimit = 5).recovered, expected)
      assert(!Files.exists(path.resolveSibling("producer-state.checkpoint.next")))
    finally deleteTree(directory)
  }

  test("recovers exact duplicate entries from a legacy checkpoint but rejects conflicts") {
    val directory = Files.createTempDirectory("cascade-producer-state-legacy-duplicate-test")
    val path = directory.resolve("producer-state.checkpoint")
    try
      val original = batch(7L, 9L, 2, 14, transactional = true)
      writeLegacyCheckpoint(path, Vector(original, original))
      assertEquals(ProducerStateCheckpoint(path, historyLimit = 5).recovered, Vector(original))

      writeLegacyCheckpoint(path, Vector(original, original.copy(lastSequence = 15)))
      val error = intercept[ProtocolException](ProducerStateCheckpoint(path, historyLimit = 5))
      assert(error.getMessage.contains("conflicts for producer 9 at offset 7"))
    finally deleteTree(directory)
  }

  private def batch(
      offset: Long,
      producerId: Long,
      epoch: Short,
      sequence: Int,
      transactional: Boolean = false,
      control: Boolean = false
  ): RecordBatchMetadata =
    RecordBatchMetadata(
      offset,
      offset,
      producerId,
      epoch,
      sequence,
      sequence,
      1,
      transactional,
      control,
      maxTimestamp = 1000L + offset,
      compressionType = 0
    )

  private def writeLegacyCheckpoint(path: java.nio.file.Path, entries: Vector[RecordBatchMetadata]): Unit =
    val bytes = 12 + entries.size * 52 + 4
    val buffer = ByteBuffer.allocate(bytes)
    buffer.putInt(0x43505331)
    buffer.putShort(1.toShort)
    buffer.putShort(0.toShort)
    buffer.putInt(entries.size)
    entries.foreach { value =>
      buffer.putLong(value.baseOffset)
      buffer.putLong(value.lastOffset)
      buffer.putLong(value.producerId)
      buffer.putShort(value.producerEpoch)
      val flags = (if value.transactional then 1 else 0) | (if value.control then 2 else 0)
      buffer.putShort(flags.toShort)
      buffer.putInt(value.baseSequence)
      buffer.putInt(value.lastSequence)
      buffer.putInt(value.recordCount)
      buffer.putLong(value.maxTimestamp)
      buffer.putInt(value.compressionType)
    }
    val checksum = CRC32C()
    checksum.update(buffer.array(), 0, bytes - 4)
    buffer.putInt(checksum.getValue.toInt)
    Files.write(path, buffer.array()): Unit

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

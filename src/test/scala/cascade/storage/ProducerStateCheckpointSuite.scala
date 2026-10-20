package cascade.storage

import cascade.protocol.ProtocolException
import java.nio.file.Files
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

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

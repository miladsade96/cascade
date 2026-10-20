package cascade.storage

import cascade.protocol.ProtocolException
import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class PartitionCleanupCheckpointSuite extends FunSuite:
  test("cleanup decision ordering rejects stale epochs and generations") {
    val first = decision(epoch = 3, generation = 2, deleteBefore = 10L)
    val state = PartitionCleanupState.Empty.prepare(first).commit(first).markApplied(first)
    assertEquals(state.nextGeneration(3), 3L)
    assertEquals(state.nextGeneration(4), 1L)
    intercept[ProtocolException](state.prepare(decision(epoch = 3, generation = 1, deleteBefore = 5L)))
    intercept[ProtocolException](state.prepare(decision(epoch = 2, generation = 99, deleteBefore = 20L)))
  }

  test("cleanup commit must match the durable prepared decision") {
    val first = decision(epoch = 1, generation = 1, deleteBefore = 2L)
    val other = decision(epoch = 1, generation = 2, deleteBefore = 3L)
    val prepared = PartitionCleanupState.Empty.prepare(first)
    intercept[ProtocolException](prepared.commit(other))
    intercept[ProtocolException](prepared.markApplied(first))
  }

  test("a newer prepare can coexist with the last committed and applied generation") {
    val first = decision(epoch = 1, generation = 1, deleteBefore = 2L)
    val second = decision(epoch = 1, generation = 2, deleteBefore = 3L)
    val previous = PartitionCleanupState.Empty.prepare(first).commit(first).markApplied(first)
    val prepared = previous.prepare(second)
    assertEquals(prepared.committed, Some(first))
    assertEquals(prepared.applied, Some(first))
    val committed = prepared.commit(second)
    assertEquals(committed.committed, Some(second))
    assertEquals(committed.applied, Some(first))
    assertEquals(committed.markApplied(second).applied, Some(second))
  }

  test("cleanup checkpoint round trips prepared committed and applied state") {
    val directory = Files.createTempDirectory("cascade-cleanup-checkpoint")
    val path = directory.resolve("replicated-cleanup.checkpoint")
    try
      val value = decision(epoch = 7, generation = 11, deleteBefore = 20L)
      val state = PartitionCleanupState.Empty.prepare(value).commit(value).markApplied(value)
      PartitionCleanupCheckpoint(path).persist(state)
      assertEquals(PartitionCleanupCheckpoint(path).recovered, state)
    finally deleteTree(directory)
  }

  test("cleanup checkpoint fails closed on CRC corruption") {
    val directory = Files.createTempDirectory("cascade-cleanup-checkpoint-crc")
    val path = directory.resolve("replicated-cleanup.checkpoint")
    try
      val value = decision(epoch = 2, generation = 1, deleteBefore = 4L)
      PartitionCleanupCheckpoint(path).persist(PartitionCleanupState.Empty.prepare(value))
      Files.write(path, Array(1.toByte), StandardOpenOption.WRITE)
      intercept[ProtocolException](PartitionCleanupCheckpoint(path))
    finally deleteTree(directory)
  }

  private def decision(epoch: Int, generation: Long, deleteBefore: Long): PartitionCleanupDecision =
    PartitionCleanupDecision(epoch, generation, PartitionCleanupPlan(deleteBefore, deleteBefore + 5L, 1234L))

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

package cascade.coordinator

import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite

final class CoordinatorShardJournalSuite extends FunSuite:
  private val shard = 7
  private val transaction = CoordinatorTransactionId(10L, 20L)
  private val delta = CoordinatorDelta(2L, Vector(CoordinatorShardUpdate(shard, 0L, Vector(1, 2, 3).map(_.toByte))))

  test("forces and recovers an ordered shard transaction") {
    val directory = Files.createTempDirectory("cascade-shard-journal")
    val path = CoordinatorShardJournal.path(directory, shard)
    val records = Vector(
      CoordinatorQuorumRecord.prepare(transaction, delta),
      CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Decide),
      CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Finalize)
    )
    val journal = CoordinatorShardJournal(path, shard)
    try records.foreach(journal.append)
    finally journal.close()

    val recovered = CoordinatorShardJournal(path, shard)
    try
      assertEquals(recovered.entries, records)
      assertEquals(recovered.snapshot.bytes, Files.size(path))
      assert(recovered.snapshot.bytes > records.map(CoordinatorQuorumRecordCodec.encode(_).length.toLong).sum)
    finally recovered.close()
  }

  test("truncates an incomplete tail without losing forced records") {
    val directory = Files.createTempDirectory("cascade-shard-torn-tail")
    val path = CoordinatorShardJournal.path(directory, shard)
    val first = CoordinatorQuorumRecord.prepare(transaction, delta)
    val journal = CoordinatorShardJournal(path, shard)
    try journal.append(first)
    finally journal.close()
    val goodSize = Files.size(path)
    Files.write(path, Array[Byte](0, 0, 0, 20, 1, 2, 3), StandardOpenOption.APPEND)

    val recovered = CoordinatorShardJournal(path, shard)
    try
      assertEquals(recovered.entries, Vector(first))
      assertEquals(recovered.snapshot.bytes, goodSize)
      assertEquals(recovered.snapshot.truncatedBytes, 7L)
    finally recovered.close()
  }

  test("rejects a prepare that does not contain its shard") {
    val directory = Files.createTempDirectory("cascade-shard-foreign")
    val journal = CoordinatorShardJournal(CoordinatorShardJournal.path(directory, shard), shard)
    val foreign = delta.copy(updates = Vector(CoordinatorShardUpdate(shard + 1, 0L, Vector.empty)))
    try intercept[IllegalArgumentException](journal.append(CoordinatorQuorumRecord.prepare(transaction, foreign)))
    finally journal.close()
  }

  test("atomically replaces history with a forced shard checkpoint") {
    val directory = Files.createTempDirectory("cascade-shard-checkpoint")
    val path = CoordinatorShardJournal.path(directory, shard)
    val journal = CoordinatorShardJournal(path, shard)
    try
      (0 until 12).foreach { index =>
        journal.append(CoordinatorQuorumRecord.prepare(
          CoordinatorTransactionId(index.toLong + 1L, index.toLong + 2L),
          delta
        ))
      }
      val before = journal.snapshot.bytes
      val checkpoint = CoordinatorQuorumRecord.checkpoint(
        CoordinatorShardCheckpoint(shard, 7L, 9L, 3L, Vector(4, 5, 6).map(_.toByte))
      )
      journal.replace(Vector(checkpoint))
      assertEquals(journal.entries, Vector(checkpoint))
      assert(journal.snapshot.bytes < before)
      assertEquals(journal.snapshot.compactions, 1L)
      assert(journal.snapshot.reclaimedBytes > 0L)
      val paths = Files.list(directory)
      try assertEquals(paths.filter(_.getFileName.toString.endsWith(".checkpoint")).count(), 0L)
      finally paths.close()
    finally journal.close()

    val recovered = CoordinatorShardJournal(path, shard)
    try assertEquals(recovered.entries.head.checkpoint.map(_.shardVersion), Some(7L))
    finally recovered.close()
  }

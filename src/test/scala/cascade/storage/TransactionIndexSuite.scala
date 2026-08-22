package cascade.storage

import cascade.TestRecordBatch
import munit.FunSuite

final class TransactionIndexSuite extends FunSuite:
  test("tracks transactional ranges and ignores ordinary batches") {
    val index = TransactionIndex()
    index.append(RecordBatch.metadata(TestRecordBatch.single(offset = 0L)))
    index.append(RecordBatch.metadata(TestRecordBatch.producer(12L, 1, 0, transactional = true, recordCount = 2)))

    assertEquals(
      index.entries,
      Vector(TransactionIndexEntry(0L, 1L, 12L, control = false))
    )
    assertEquals(index.overlapping(1L, 2L), index.entries)
    assertEquals(index.overlapping(2L, 3L), Vector.empty)
  }

  test("rejects overlapping transactional ranges") {
    val index = TransactionIndex()
    val metadata = RecordBatch.metadata(TestRecordBatch.producer(12L, 1, 0, transactional = true))
    index.append(metadata)
    intercept[IllegalArgumentException](index.append(metadata))
  }

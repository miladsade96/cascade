package cascade.storage

import cascade.TestRecordBatch
import munit.FunSuite

final class TimestampIndexSuite extends FunSuite:
  test("finds the first batch whose maximum timestamp reaches the target") {
    val index = TimestampIndex()
    index.append(RecordBatch.metadata(TestRecordBatch.keyed(Vector(record(1000L)), baseOffset = 10L)))
    index.append(RecordBatch.metadata(TestRecordBatch.keyed(Vector(record(1020L)), baseOffset = 11L)))
    index.append(RecordBatch.metadata(TestRecordBatch.keyed(Vector(record(1010L)), baseOffset = 12L)))

    assertEquals(index.offsetFor(999L), Some(10L))
    assertEquals(index.offsetFor(1001L), Some(11L))
    assertEquals(index.offsetFor(1021L), None)
    assertEquals(index.entries.map(_.baseOffset), Vector(10L, 11L, 12L))
  }

  test("rejects duplicate or decreasing batch offsets") {
    val index = TimestampIndex()
    val metadata = RecordBatch.metadata(TestRecordBatch.keyed(Vector(record(1000L)), baseOffset = 2L))
    index.append(metadata)
    intercept[IllegalArgumentException](index.append(metadata))
  }

  private def record(timestamp: Long): TestRecordBatch.Record =
    TestRecordBatch.Record(Some("key".getBytes), Some("value".getBytes), timestamp)

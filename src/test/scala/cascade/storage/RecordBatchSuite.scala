package cascade.storage

import cascade.TestRecordBatch
import cascade.protocol.ProtocolException
import java.nio.{ByteBuffer, ByteOrder}
import munit.FunSuite

final class RecordBatchSuite extends FunSuite:
  test("producer sequence metadata wraps after the maximum Kafka sequence") {
    val batch = TestRecordBatch.producer(
      producerId = 10L,
      producerEpoch = 3,
      baseSequence = Int.MaxValue,
      transactional = true,
      recordCount = 2
    )
    val metadata = RecordBatch.metadata(batch)
    assertEquals(metadata.baseSequence, Int.MaxValue)
    assertEquals(metadata.lastSequence, 0)
    assertEquals(metadata.recordCount, 2)
    assertEquals(metadata.lastOffset, 1L)
    assert(metadata.transactional)
    assert(!metadata.control)
  }

  test("record-set preparation rejects truncated and negative batch counters") {
    intercept[ProtocolException](RecordBatch.prepare(Array.fill[Byte](11)(0), 0L))

    val invalid = TestRecordBatch.single()
    ByteBuffer.wrap(invalid).order(ByteOrder.BIG_ENDIAN).putInt(57, -1)
    intercept[ProtocolException](RecordBatch.prepare(invalid, 0L))
  }

  test("indexes timestamps, keys, tombstones, and absolute offsets from uncompressed records") {
    val batch = TestRecordBatch.keyed(
      Vector(
        TestRecordBatch.Record(Some("alpha".getBytes), Some("one".getBytes), 1000L),
        TestRecordBatch.Record(Some("beta".getBytes), None, 1010L)
      ),
      baseOffset = 20L
    )

    val metadata = RecordBatch.metadata(batch)
    assertEquals(metadata.maxTimestamp, 1010L)
    assertEquals(metadata.compressionType, 0)
    assertEquals(
      RecordBatch.indexedRecords(batch).getOrElse(fail("records were not decoded")),
      Vector(
        IndexedRecord(20L, 1000L, Some("alpha".getBytes.toVector), tombstone = false),
        IndexedRecord(21L, 1010L, Some("beta".getBytes.toVector), tombstone = true)
      )
    )
  }

  test("keeps compressed and malformed record payloads opaque") {
    val compressed = TestRecordBatch.keyed(
      Vector(TestRecordBatch.Record(Some("key".getBytes), Some("value".getBytes), 1000L))
    )
    ByteBuffer.wrap(compressed).order(ByteOrder.BIG_ENDIAN).putShort(21, 1.toShort)
    assertEquals(RecordBatch.indexedRecords(compressed), None)
    assertEquals(RecordBatch.indexedRecords(TestRecordBatch.single()), None)
  }

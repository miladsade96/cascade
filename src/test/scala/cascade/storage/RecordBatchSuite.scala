package cascade.storage

import cascade.TestRecordBatch
import cascade.protocol.ProtocolException
import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.CRC32C
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

  test("rejects CRC corruption before assigning broker offsets") {
    val batch = TestRecordBatch.keyed(
      Vector(TestRecordBatch.Record(Some("key".getBytes), Some("value".getBytes), 1000L))
    )
    batch(batch.length - 1) = (batch.last ^ 0x01).toByte

    val error = intercept[ProtocolException](RecordBatch.prepare(batch, 42L))
    assert(error.getMessage.contains("CRC32C mismatch"))
    assertEquals(ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN).getLong(0), 0L)
  }

  test("excludes the broker-assigned base offset from Kafka CRC coverage") {
    val batch = TestRecordBatch.keyed(
      Vector(TestRecordBatch.Record(Some("key".getBytes), Some("value".getBytes), 1000L)),
      baseOffset = Long.MaxValue
    )
    val crc = RecordBatch.storedCrc(batch)

    val prepared = RecordBatch.prepare(batch, 17L).head

    assertEquals(prepared.baseOffset, 17L)
    assertEquals(RecordBatch.storedCrc(prepared.bytes), crc)
    assert(RecordBatch.hasValidCrc(prepared.bytes))
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

  test("compacts and rebuilds every Kafka compression codec with valid offsets and CRC") {
    (1 to 4).foreach { compressionType =>
      val batch = TestRecordBatch.compressedKeyed(
        Vector(
          TestRecordBatch.Record(Some("key".getBytes), Some("old".getBytes), 1000L),
          TestRecordBatch.Record(Some("key".getBytes), Some("new".getBytes), 1010L)
        ),
        compressionType,
        baseOffset = 20L
      )
      val inputHeader = ByteBuffer.wrap(batch).order(ByteOrder.BIG_ENDIAN)
      inputHeader.putLong(43, 99L)
      inputHeader.putShort(51, 2.toShort)
      inputHeader.putInt(53, 100)
      val inputCrc = CRC32C()
      inputCrc.update(batch, 21, batch.length - 21)
      inputHeader.putInt(17, inputCrc.getValue.toInt)

      assertEquals(
        RecordBatch.indexedRecords(batch).getOrElse(fail(s"codec $compressionType was not decoded")).map(_.offset),
        Vector(20L, 21L)
      )
      val compacted = RecordBatch.compact(batch, Map("key".getBytes.toVector -> 21L), Long.MinValue)
        .getOrElse(fail(s"codec $compressionType was removed"))
      val metadata = RecordBatch.metadata(compacted)
      assertEquals(metadata.baseOffset, 20L)
      assertEquals(metadata.lastOffset, 21L)
      assertEquals(metadata.recordCount, 1)
      assertEquals(metadata.compressionType, compressionType)
      assertEquals(metadata.baseSequence, 100)
      assertEquals(metadata.lastSequence, 101)
      assertEquals(
        RecordBatch.indexedRecords(compacted).getOrElse(fail(s"codec $compressionType rewrite was unreadable")).map(_.offset),
        Vector(21L)
      )

      val expectedCrc = CRC32C()
      expectedCrc.update(compacted, 21, compacted.length - 21)
      assertEquals(ByteBuffer.wrap(compacted).order(ByteOrder.BIG_ENDIAN).getInt(17), expectedCrc.getValue.toInt)
    }
  }

  test("keeps malformed compressed record payloads opaque") {
    val compressed = TestRecordBatch.keyed(
      Vector(TestRecordBatch.Record(Some("key".getBytes), Some("value".getBytes), 1000L))
    )
    ByteBuffer.wrap(compressed).order(ByteOrder.BIG_ENDIAN).putShort(21, 1.toShort)
    assertEquals(RecordBatch.indexedRecords(compressed), None)
    assertEquals(RecordBatch.indexedRecords(TestRecordBatch.single()), None)
  }

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

package cascade

import java.nio.{ByteBuffer, ByteOrder}

object TestRecordBatch:
  def single(offset: Long = 0L, totalBytes: Int = 61): Array[Byte] =
    require(totalBytes >= 61, "record batch must contain the complete magic-v2 header")
    val buffer = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(offset)
    buffer.putInt(totalBytes - 12) // bytes after batchLength
    buffer.putInt(0) // partition leader epoch
    buffer.put(2.toByte)
    buffer.putInt(0) // CRC is opaque to storage tests
    buffer.putShort(0.toShort)
    buffer.putInt(0) // last offset delta
    buffer.putLong(0L)
    buffer.putLong(0L)
    buffer.putLong(-1L)
    buffer.putShort(-1.toShort)
    buffer.putInt(-1)
    buffer.putInt(1)
    buffer.array()

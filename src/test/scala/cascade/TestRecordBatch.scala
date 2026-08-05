package cascade

import java.nio.{ByteBuffer, ByteOrder}

object TestRecordBatch:
  def single(offset: Long = 0L): Array[Byte] =
    val buffer = ByteBuffer.allocate(61).order(ByteOrder.BIG_ENDIAN)
    buffer.putLong(offset)
    buffer.putInt(49) // bytes after batchLength
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


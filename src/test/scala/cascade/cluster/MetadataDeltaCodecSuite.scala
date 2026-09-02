package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import munit.FunSuite

final class MetadataDeltaCodecSuite extends FunSuite:
  private val delta = MetadataDeltaFixture.update(MetadataDeltaFixture.base)._1

  test("delta envelopes round trip without being valid legacy metadata") {
    val bytes = MetadataDeltaCodec.encode(delta)
    assert(MetadataDeltaCodec.isDelta(bytes))
    assertEquals(ByteCursor(bytes).readShort(), (-10).toShort)
    assertEquals(MetadataDeltaCodec.decode(bytes), delta)
    intercept[ProtocolException](MetadataCodec.decode(bytes))
  }

  test("truncated trailing wrong-format and duplicate-shard envelopes fail closed") {
    val bytes = MetadataDeltaCodec.encode(delta)
    (0 until bytes.length).foreach(size => intercept[RuntimeException](MetadataDeltaCodec.decode(bytes.take(size))))
    intercept[ProtocolException](MetadataDeltaCodec.decode(bytes :+ 0.toByte))
    intercept[ProtocolException](MetadataDeltaCodec.decode(bytes.updated(0, 0.toByte)))
    val update = delta.change.updates.head
    val duplicate = ByteWriter().writeShort(-10).writeLong(10L).writeLong(0L).writeBytes(delta.baseFingerprint.toArray).writeShort(1).writeLong(4L).writeInt(2)
    (0 until 2).foreach { _ => duplicate.writeInt(update.id).writeLong(update.expectedVersion).writeByteArray(update.payload.toArray): Unit }
    intercept[IllegalArgumentException](MetadataDeltaCodec.decode(duplicate.result()))
  }

  test("negative bases and oversized frames are rejected before decoding contents") {
    intercept[IllegalArgumentException](delta.copy(baseVersion = -1L))
    intercept[ProtocolException](MetadataDeltaCodec.decode(new Array[Byte](MetadataDeltaCodec.MaximumBytes + 1)))
  }

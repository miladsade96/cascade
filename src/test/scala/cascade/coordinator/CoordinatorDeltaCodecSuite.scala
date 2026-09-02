package cascade.coordinator

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import munit.FunSuite

final class CoordinatorDeltaCodecSuite extends FunSuite:
  private val update = CoordinatorShardUpdate(1, 2L, Vector[Byte](3, 4))
  test("delta wire format round-trips and canonicalizes shard order") {
    val delta = CoordinatorDelta(9L, Vector(update.copy(id = 2), update))
    assertEquals(CoordinatorDeltaCodec.decode(ByteCursor(CoordinatorDeltaCodec.encode(delta))), delta.copy(updates = delta.updates.reverse))
  }
  test("wire format rejects oversized counts, trailing bytes, and duplicate shards") {
    intercept[ProtocolException](CoordinatorDeltaCodec.decode(ByteCursor(ByteWriter().writeShort(1).writeLong(1).writeInt(130).result())))
    val bytes = CoordinatorDeltaCodec.encode(CoordinatorDelta(1L, Vector(update)))
    intercept[ProtocolException](CoordinatorDeltaCodec.decode(ByteCursor(bytes :+ 0.toByte)))
    intercept[IllegalArgumentException](CoordinatorDelta(1L, Vector(update, update)))
    intercept[IllegalArgumentException](CoordinatorShardUpdate(-1, 0L, Vector.empty))
  }

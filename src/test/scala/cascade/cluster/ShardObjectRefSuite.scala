package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter}
import munit.FunSuite

final class ShardObjectRefSuite extends FunSuite:
  test("bounded references round trip and bind content to its shard") {
    val ref = ShardObjectRef.identify(3, Array[Byte](1, 2))
    val writer = ByteWriter()
    ShardObjectRef.write(writer, ref)
    val cursor = ByteCursor(writer.result())
    assertEquals(ShardObjectRef.read(cursor), ref)
    cursor.ensureFullyRead()
    assertNotEquals(ref.digest, ShardObjectRef.identify(4, Array[Byte](1, 2)).digest)
    assertNotEquals(ref.digest, ShardObjectRef.identify(3, Array[Byte](1, 3)).digest)
    assert(ref.fileName.matches("3-[0-9a-f]{64}\\.shard"))
  }

  test("invalid namespaces sizes and digests cannot form file paths") {
    for id <- Vector(-1, ShardObjectRef.DeliverySnapshot + 1) do
      intercept[IllegalArgumentException](ShardObjectRef(id, 0, Vector.fill(32)(0.toByte)))
    for size <- Vector(-1, ShardObjectRef.MaximumBytes + 1) do
      intercept[IllegalArgumentException](ShardObjectRef(0, size, Vector.fill(32)(0.toByte)))
    intercept[IllegalArgumentException](ShardObjectRef(0, 0, Vector.empty))
  }

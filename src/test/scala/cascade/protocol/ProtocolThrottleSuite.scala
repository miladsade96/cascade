package cascade.protocol

import munit.FunSuite

final class ProtocolThrottleSuite extends FunSuite:
  test("adds delay to Fetch's leading throttle field") {
    val header = RequestHeader(ApiKey.Fetch, 6, 7, Some("test"))
    val response = ResponseFrame.encode(header, ByteWriter().writeInt(5).writeArray(Vector.empty[Int])(_ => ()).result())
    ProtocolThrottle.add(response, ApiKey.Fetch, 20L)
    val cursor = ByteCursor(response)
    cursor.readInt()
    cursor.readInt()
    assertEquals(cursor.readInt(), 25)
  }

  test("adds delay to Produce's trailing throttle field and ignores APIs without a supported field") {
    val header = RequestHeader(ApiKey.Produce, 3, 8, Some("test"))
    val response = ResponseFrame.encode(header, ByteWriter().writeArray(Vector.empty[Int])(_ => ()).writeInt(7).result())
    ProtocolThrottle.add(response, ApiKey.Produce, 30L)
    val cursor = ByteCursor(response)
    cursor.readInt()
    cursor.readInt()
    cursor.readArray(cursor.readInt())
    assertEquals(cursor.readInt(), 37)
    val unchanged = response.clone()
    ProtocolThrottle.add(unchanged, ApiKey.Metadata, 100L)
    assertEquals(unchanged.toVector, response.toVector)
  }

package cascade.protocol

import munit.FunSuite

final class BinaryCodecSuite extends FunSuite:
  test("round-trips fixed-width, string, compact, nullable, array, and tagged encodings") {
    val writer = ByteWriter()
    writer.writeByte(7)
    writer.writeShort(32000)
    writer.writeInt(0x12345678)
    writer.writeLong(0x123456789abcdef0L)
    writer.writeString("Kafka λ")
    writer.writeNullableString(None)
    writer.writeCompactString("Scala")
    writer.writeCompactNullableString(Some("wire"))
    writer.writeArray(Vector(1, 2, 3))(writer.writeInt)
    writer.writeEmptyTaggedFields()

    val cursor = ByteCursor(writer.result())
    assertEquals(cursor.readByte(), 7.toByte)
    assertEquals(cursor.readShort(), 32000.toShort)
    assertEquals(cursor.readInt(), 0x12345678)
    assertEquals(cursor.readLong(), 0x123456789abcdef0L)
    assertEquals(cursor.readString(), "Kafka λ")
    assertEquals(cursor.readNullableString(), None)
    assertEquals(cursor.readCompactString(), "Scala")
    assertEquals(cursor.readCompactNullableString(), Some("wire"))
    assertEquals(cursor.readArray(cursor.readInt()), Vector(1, 2, 3))
    cursor.skipTaggedFields()
    cursor.ensureFullyRead()
  }

  test("round-trips nullable compact arrays used by reassignment APIs") {
    val writer = ByteWriter()
    writer.writeCompactNullableArray(Some(Vector(1, 3, 5)))(value => writer.writeInt(value): Unit)
    writer.writeCompactNullableArray[Int](None)(_ => ())
    val cursor = ByteCursor(writer.result())
    assertEquals(cursor.readCompactNullableArray(cursor.readInt()), Some(Vector(1, 3, 5)))
    assertEquals(cursor.readCompactNullableArray(cursor.readInt()), None)
    cursor.ensureFullyRead()
  }

  test("rejects truncated input before accessing outside the frame") {
    val cursor = ByteCursor(Array[Byte](0, 0, 0))
    intercept[ProtocolException](cursor.readInt())
  }

  test("rejects unordered flexible tagged fields") {
    val writer = ByteWriter()
    writer.writeUnsignedVarInt(2)
    writer.writeUnsignedVarInt(3).writeUnsignedVarInt(0)
    writer.writeUnsignedVarInt(2).writeUnsignedVarInt(0)
    intercept[ProtocolException](ByteCursor(writer.result()).skipTaggedFields())
  }

  test("request header v2 retains a nullable non-compact client id") {
    val frame = ByteWriter()
      .writeShort(ApiKey.ApiVersions)
      .writeShort(4)
      .writeInt(99)
      .writeNullableString(Some("client-v2"))
      .writeEmptyTaggedFields()
      .writeCompactString("software")
      .result()
    val (header, body) = RequestHeader.decode(frame)
    assertEquals(header.correlationId, 99)
    assertEquals(header.clientId, Some("client-v2"))
    assertEquals(body.readCompactString(), "software")
  }

  test("round-trips unsigned ports and Kafka UUIDs") {
    val bytes = ByteWriter()
      .writeShort(65535)
      .writeUuid(0x1020304050607080L, 0x0102030405060708L)
      .result()
    val cursor = ByteCursor(bytes)

    assertEquals(cursor.readUnsignedShort(), 65535)
    assertEquals(cursor.readUuid(), (0x1020304050607080L, 0x0102030405060708L))
    cursor.ensureFullyRead()
  }

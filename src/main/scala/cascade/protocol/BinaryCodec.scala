package cascade.protocol

import java.nio.charset.StandardCharsets
import java.util.Arrays

final class ProtocolException(message: String) extends RuntimeException(message)

/** A bounds-checked, allocation-conscious Kafka protocol reader. */
final class ByteCursor(private val bytes: Array[Byte]):
  private val MaximumCollectionElements = 1_000_000
  private var position = 0

  def remaining: Int = bytes.length - position
  def offset: Int = position

  private def requireBytes(count: Int): Unit =
    if count < 0 || count > remaining then
      throw ProtocolException(s"need $count bytes, only $remaining remain at offset $position")

  def readByte(): Byte =
    requireBytes(1)
    val value = bytes(position)
    position += 1
    value

  def readBoolean(): Boolean = readByte() != 0

  def readShort(): Short =
    requireBytes(2)
    val value = (((bytes(position) & 0xff) << 8) | (bytes(position + 1) & 0xff)).toShort
    position += 2
    value

  def readInt(): Int =
    requireBytes(4)
    val value =
      ((bytes(position) & 0xff) << 24) |
        ((bytes(position + 1) & 0xff) << 16) |
        ((bytes(position + 2) & 0xff) << 8) |
        (bytes(position + 3) & 0xff)
    position += 4
    value

  def readLong(): Long =
    requireBytes(8)
    val high = readInt().toLong & 0xffffffffL
    val low = readInt().toLong & 0xffffffffL
    (high << 32) | low

  def readUnsignedVarInt(): Int =
    var value = 0
    var shift = 0
    while shift < 35 do
      val current = readByte() & 0xff
      if shift == 28 && (current & 0x78) != 0 then
        throw ProtocolException("unsigned varint exceeds signed JVM integer range")
      value |= (current & 0x7f) << shift
      if (current & 0x80) == 0 then return value
      shift += 7
    throw ProtocolException("unsigned varint is longer than five bytes")

  def readString(): String =
    val length = readShort().toInt
    if length < 0 then throw ProtocolException("non-null string has a negative length")
    decodeUtf8(readBytes(length))

  def readNullableString(): Option[String] =
    val length = readShort().toInt
    if length == -1 then None
    else if length < -1 then throw ProtocolException(s"invalid nullable string length: $length")
    else Some(decodeUtf8(readBytes(length)))

  def readCompactString(): String =
    val encodedLength = readUnsignedVarInt()
    if encodedLength == 0 then throw ProtocolException("non-null compact string is null")
    decodeUtf8(readBytes(encodedLength - 1))

  def readCompactNullableString(): Option[String] =
    val encodedLength = readUnsignedVarInt()
    if encodedLength == 0 then None else Some(decodeUtf8(readBytes(encodedLength - 1)))

  def readBytes(length: Int): Array[Byte] =
    requireBytes(length)
    val value = Arrays.copyOfRange(bytes, position, position + length)
    position += length
    value

  def readNullableBytes(): Option[Array[Byte]] =
    val length = readInt()
    if length == -1 then None
    else if length < -1 then throw ProtocolException(s"invalid nullable bytes length: $length")
    else Some(readBytes(length))

  def readArray[A](readElement: => A): Vector[A] =
    val length = readInt()
    if length < 0 then throw ProtocolException(s"non-null array has invalid length: $length")
    validateCollectionLength(length)
    Vector.fill(length)(readElement)

  def readNullableArray[A](readElement: => A): Option[Vector[A]] =
    val length = readInt()
    if length == -1 then None
    else if length < -1 then throw ProtocolException(s"nullable array has invalid length: $length")
    else
      validateCollectionLength(length)
      Some(Vector.fill(length)(readElement))

  def readCompactArray[A](readElement: => A): Vector[A] =
    val encodedLength = readUnsignedVarInt()
    if encodedLength == 0 then throw ProtocolException("non-null compact array is null")
    val length = encodedLength - 1
    validateCollectionLength(length)
    Vector.fill(length)(readElement)

  def skipTaggedFields(): Unit =
    val fieldCount = readUnsignedVarInt()
    var previousTag = -1
    var index = 0
    while index < fieldCount do
      val tag = readUnsignedVarInt()
      if tag <= previousTag then throw ProtocolException("tagged fields are not strictly ordered")
      previousTag = tag
      val size = readUnsignedVarInt()
      readBytes(size)
      index += 1

  def ensureFullyRead(): Unit =
    if remaining != 0 then throw ProtocolException(s"$remaining unread bytes remain")

  private def decodeUtf8(value: Array[Byte]): String = new String(value, StandardCharsets.UTF_8)

  private def validateCollectionLength(length: Int): Unit =
    if length > MaximumCollectionElements then
      throw ProtocolException(s"collection length $length exceeds safety limit $MaximumCollectionElements")

/** A growing big-endian Kafka protocol writer. */
final class ByteWriter(initialCapacity: Int = 256):
  private var bytes = new Array[Byte](math.max(16, initialCapacity))
  private var position = 0

  def size: Int = position

  private def reserve(count: Int): Unit =
    val required = position + count
    if required > bytes.length then
      var next = bytes.length
      while next < required do next = Math.multiplyExact(next, 2)
      bytes = Arrays.copyOf(bytes, next)

  def writeByte(value: Int): this.type =
    reserve(1)
    bytes(position) = value.toByte
    position += 1
    this

  def writeBoolean(value: Boolean): this.type = writeByte(if value then 1 else 0)

  def writeShort(value: Int): this.type =
    reserve(2)
    bytes(position) = (value >>> 8).toByte
    bytes(position + 1) = value.toByte
    position += 2
    this

  def writeInt(value: Int): this.type =
    reserve(4)
    bytes(position) = (value >>> 24).toByte
    bytes(position + 1) = (value >>> 16).toByte
    bytes(position + 2) = (value >>> 8).toByte
    bytes(position + 3) = value.toByte
    position += 4
    this

  def writeLong(value: Long): this.type =
    writeInt((value >>> 32).toInt)
    writeInt(value.toInt)

  def writeUnsignedVarInt(value: Int): this.type =
    if value < 0 then throw IllegalArgumentException("unsigned varint cannot be negative")
    var remaining = value
    while (remaining & 0xffffff80) != 0 do
      writeByte((remaining & 0x7f) | 0x80)
      remaining >>>= 7
    writeByte(remaining)

  def writeString(value: String): this.type =
    val encoded = value.getBytes(StandardCharsets.UTF_8)
    if encoded.length > Short.MaxValue then throw IllegalArgumentException("Kafka string exceeds 32767 bytes")
    writeShort(encoded.length)
    writeBytes(encoded)

  def writeNullableString(value: Option[String]): this.type = value match
    case None       => writeShort(-1)
    case Some(text) => writeString(text)

  def writeCompactString(value: String): this.type =
    val encoded = value.getBytes(StandardCharsets.UTF_8)
    writeUnsignedVarInt(encoded.length + 1)
    writeBytes(encoded)

  def writeCompactNullableString(value: Option[String]): this.type = value match
    case None => writeUnsignedVarInt(0)
    case Some(text) =>
      val encoded = text.getBytes(StandardCharsets.UTF_8)
      writeUnsignedVarInt(encoded.length + 1)
      writeBytes(encoded)

  def writeBytes(value: Array[Byte]): this.type =
    reserve(value.length)
    System.arraycopy(value, 0, bytes, position, value.length)
    position += value.length
    this

  def writeNullableBytes(value: Option[Array[Byte]]): this.type = value match
    case None => writeInt(-1)
    case Some(payload) =>
      writeInt(payload.length)
      writeBytes(payload)

  def writeArray[A](values: Iterable[A])(writeElement: A => Unit): this.type =
    writeInt(values.size)
    values.foreach(writeElement)
    this

  def writeNullableArray[A](values: Option[Iterable[A]])(writeElement: A => Unit): this.type = values match
    case None => writeInt(-1)
    case Some(items) => writeArray(items)(writeElement)

  def writeCompactArray[A](values: Iterable[A])(writeElement: A => Unit): this.type =
    writeUnsignedVarInt(values.size + 1)
    values.foreach(writeElement)
    this

  def writeEmptyTaggedFields(): this.type = writeUnsignedVarInt(0)

  def result(): Array[Byte] = Arrays.copyOf(bytes, position)

package cascade.protocol

object ProtocolThrottle:
  def add(response: Array[Byte], apiKey: Short, delayMillis: Long): Unit =
    if delayMillis <= 0L then return
    val offset = apiKey match
      case ApiKey.Fetch   => 8
      case ApiKey.Produce => response.length - Integer.BYTES
      case _              => return
    if offset < 8 || offset > response.length - Integer.BYTES then
      throw ProtocolException("response is too short for a throttle field")
    val current = readInt(response, offset)
    val combined = math.min(Int.MaxValue.toLong, current.toLong + delayMillis).toInt
    writeInt(response, offset, combined)

  private def readInt(bytes: Array[Byte], offset: Int): Int =
    ((bytes(offset) & 0xff) << 24) | ((bytes(offset + 1) & 0xff) << 16) |
      ((bytes(offset + 2) & 0xff) << 8) | (bytes(offset + 3) & 0xff)

  private def writeInt(bytes: Array[Byte], offset: Int, value: Int): Unit =
    bytes(offset) = (value >>> 24).toByte
    bytes(offset + 1) = (value >>> 16).toByte
    bytes(offset + 2) = (value >>> 8).toByte
    bytes(offset + 3) = value.toByte

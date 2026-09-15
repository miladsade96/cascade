package cascade.security

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

object DistributedQuotaCodec:
  private val NoDecision = 0
  private val Allowed = 1
  private val Throttle = 2
  private val Rejected = 3

  def encodeReservation(value: ClusterQuotaReservation): Array[Byte] =
    ByteWriter()
      .writeByte(value.kind.id)
      .writeString(value.principal)
      .writeInt(value.bytes)
      .writeBoolean(value.rejectExcess)
      .writeLong(value.limit.bytesPerSecond)
      .writeLong(value.limit.burstBytes)
      .writeLong(value.limit.maxThrottleMillis)
      .writeLong(value.controllerTerm)
      .result()

  def decodeReservation(cursor: ByteCursor): ClusterQuotaReservation =
    val kindId = cursor.readByte()
    val principal = cursor.readString()
    val bytes = cursor.readInt()
    val rejectExcess = cursor.readBoolean()
    val limit = QuotaLimit(cursor.readLong(), cursor.readLong(), cursor.readLong())
    val controllerTerm = cursor.readLong()
    cursor.ensureFullyRead()
    ClusterQuotaReservation(
      QuotaKind.fromId(kindId).getOrElse(throw ProtocolException(s"unknown quota kind: $kindId")),
      principal,
      bytes,
      rejectExcess,
      limit,
      controllerTerm
    )

  def encodeResult(value: ClusterQuotaResult): Array[Byte] =
    val writer = ByteWriter().writeShort(value.errorCode).writeLong(value.controllerTerm)
    value.decision match
      case None                                  => writer.writeByte(NoDecision).writeLong(0L)
      case Some(QuotaDecision.Allowed)           => writer.writeByte(Allowed).writeLong(0L)
      case Some(QuotaDecision.Throttle(delay))   => writer.writeByte(Throttle).writeLong(delay)
      case Some(QuotaDecision.Rejected(required)) => writer.writeByte(Rejected).writeLong(required)
    writer.result()

  def decodeResult(cursor: ByteCursor): ClusterQuotaResult =
    val errorCode = cursor.readShort()
    val controllerTerm = cursor.readLong()
    val decisionType = cursor.readByte()
    val delay = cursor.readLong()
    cursor.ensureFullyRead()
    val decision = decisionType match
      case NoDecision => None
      case Allowed if delay == 0L => Some(QuotaDecision.Allowed)
      case Throttle if delay > 0L => Some(QuotaDecision.Throttle(delay))
      case Rejected if delay > 0L => Some(QuotaDecision.Rejected(delay))
      case other => throw ProtocolException(s"invalid distributed quota result: type=$other delay=$delay")
    ClusterQuotaResult(errorCode, decision, controllerTerm)

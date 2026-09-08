package cascade.coordinator

import cascade.protocol.{ByteCursor, ByteWriter}

final case class CoordinatorQuorumRequest(controllerTerm: Long, record: CoordinatorQuorumRecord):
  require(controllerTerm >= 0L, "negative coordinator quorum controller term")

object CoordinatorQuorumRequestCodec:
  def encode(request: CoordinatorQuorumRequest): Array[Byte] =
    ByteWriter()
      .writeLong(request.controllerTerm)
      .writeByteArray(CoordinatorQuorumRecordCodec.encode(request.record))
      .result()

  def decode(cursor: ByteCursor): CoordinatorQuorumRequest =
    val request = CoordinatorQuorumRequest(
      cursor.readLong(),
      CoordinatorQuorumRecordCodec.decode(cursor.readByteArray())
    )
    cursor.ensureFullyRead()
    request

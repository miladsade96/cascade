package cascade.coordinator

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

final case class CoordinatorDecisionQuery(transactionId: CoordinatorTransactionId)

final case class CoordinatorDecisionQueryResult(
    errorCode: Short,
    status: CoordinatorTransactionStatus,
    decisionVoters: Vector[Int]
):
  require(decisionVoters.distinct == decisionVoters.sorted, "decision voters must be sorted and unique")

object CoordinatorDecisionQueryCodec:
  def encode(query: CoordinatorDecisionQuery): Array[Byte] =
    ByteWriter().writeLong(query.transactionId.high).writeLong(query.transactionId.low).result()

  def decode(cursor: ByteCursor): CoordinatorDecisionQuery =
    val query = CoordinatorDecisionQuery(CoordinatorTransactionId(cursor.readLong(), cursor.readLong()))
    cursor.ensureFullyRead()
    query

  def encodeResult(result: CoordinatorDecisionQueryResult): Array[Byte] =
    val writer = ByteWriter().writeShort(result.errorCode).writeByte(result.status.id)
    writer.writeArray(result.decisionVoters)(voter => writer.writeInt(voter): Unit)
    writer.result()

  def decodeResult(cursor: ByteCursor): CoordinatorDecisionQueryResult =
    val error = cursor.readShort()
    val status =
      try CoordinatorTransactionStatus.fromId(cursor.readByte())
      catch case exception: IllegalArgumentException => throw ProtocolException(exception.getMessage)
    val voters = cursor.readArray(cursor.readInt()).sorted
    cursor.ensureFullyRead()
    CoordinatorDecisionQueryResult(error, status, voters)

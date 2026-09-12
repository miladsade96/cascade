package cascade.coordinator

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

final case class CoordinatorDecisionQuery(transactionId: CoordinatorTransactionId)

final case class CoordinatorDecisionQueryResult(
    errorCode: Short,
    status: CoordinatorTransactionStatus,
    certificate: Option[CoordinatorDecisionCertificate]
):
  require(
    (status == CoordinatorTransactionStatus.Committed || status == CoordinatorTransactionStatus.Finalized) || certificate.isEmpty,
    "only committed coordinator states carry a certificate"
  )

object CoordinatorDecisionQueryCodec:
  def encode(query: CoordinatorDecisionQuery): Array[Byte] =
    ByteWriter().writeLong(query.transactionId.high).writeLong(query.transactionId.low).result()

  def decode(cursor: ByteCursor): CoordinatorDecisionQuery =
    val query = CoordinatorDecisionQuery(CoordinatorTransactionId(cursor.readLong(), cursor.readLong()))
    cursor.ensureFullyRead()
    query

  def encodeResult(result: CoordinatorDecisionQueryResult): Array[Byte] =
    val writer = ByteWriter().writeShort(result.errorCode).writeByte(result.status.id).writeBoolean(result.certificate.nonEmpty)
    result.certificate.foreach(value => CoordinatorDecisionCertificateCodec.write(writer, value))
    writer.result()

  def decodeResult(cursor: ByteCursor): CoordinatorDecisionQueryResult =
    val error = cursor.readShort()
    val status =
      try CoordinatorTransactionStatus.fromId(cursor.readByte())
      catch case exception: IllegalArgumentException => throw ProtocolException(exception.getMessage)
    val certificate = Option.when(cursor.readBoolean())(CoordinatorDecisionCertificateCodec.read(cursor))
    cursor.ensureFullyRead()
    CoordinatorDecisionQueryResult(error, status, certificate)

package cascade.coordinator

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

object CoordinatorQuorumRecordCodec:
  val Format: Short = 2
  private val LegacyFormat: Short = 1
  val MaximumBytes: Int = 128 * 1024 * 1024

  def encode(record: CoordinatorQuorumRecord): Array[Byte] =
    val writer = ByteWriter()
      .writeShort(Format)
      .writeByte(record.phase.id)
      .writeLong(record.transactionId.high)
      .writeLong(record.transactionId.low)
    record.delta.foreach(delta => writer.writeByteArray(CoordinatorDeltaCodec.encode(delta)): Unit)
    record.certificate.foreach(certificate => CoordinatorDecisionCertificateCodec.write(writer, certificate))
    val bytes = writer.result()
    if bytes.length > MaximumBytes then throw ProtocolException("coordinator quorum record exceeds size limit")
    bytes

  def decode(bytes: Array[Byte]): CoordinatorQuorumRecord =
    if bytes.length > MaximumBytes then throw ProtocolException("coordinator quorum record exceeds size limit")
    val cursor = ByteCursor(bytes)
    val format = cursor.readShort()
    if format != LegacyFormat && format != Format then throw ProtocolException("unsupported coordinator quorum record format")
    val phase =
      try CoordinatorQuorumPhase.fromId(cursor.readByte())
      catch case error: IllegalArgumentException => throw ProtocolException(error.getMessage)
    val transactionId = CoordinatorTransactionId(cursor.readLong(), cursor.readLong())
    if format == LegacyFormat && phase.id > CoordinatorQuorumPhase.Abort.id then
      throw ProtocolException("legacy coordinator record contains a new phase")
    val delta = Option.when(phase == CoordinatorQuorumPhase.Prepare || phase == CoordinatorQuorumPhase.Recover) {
      CoordinatorDeltaCodec.decode(ByteCursor(cursor.readByteArray()))
    }
    val certificate = Option.when(phase == CoordinatorQuorumPhase.Commit || phase == CoordinatorQuorumPhase.Recover) {
      CoordinatorDecisionCertificateCodec.read(cursor)
    }
    cursor.ensureFullyRead()
    CoordinatorQuorumRecord(transactionId, phase, delta, certificate)

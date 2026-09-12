package cascade.coordinator

import cascade.cluster.VoterDirectoryId
import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

object CoordinatorDecisionCertificateCodec:
  def write(writer: ByteWriter, certificate: CoordinatorDecisionCertificate): Unit =
    writeVoters(writer, certificate.currentVoters)
    writeVoters(writer, certificate.nextVoters)
    writer.writeArray(certificate.acknowledgedNodeIds)(nodeId => writer.writeInt(nodeId): Unit)

  def read(cursor: ByteCursor): CoordinatorDecisionCertificate =
    try
      CoordinatorDecisionCertificate(
        readVoters(cursor),
        readVoters(cursor),
        cursor.readArray(cursor.readInt()).sorted
      )
    catch case exception: IllegalArgumentException => throw ProtocolException(exception.getMessage)

  private def writeVoters(writer: ByteWriter, voters: Vector[CoordinatorCertificateVoter]): Unit =
    writer.writeArray(voters) { voter =>
      writer.writeInt(voter.nodeId)
        .writeLong(voter.directoryId.mostSignificantBits)
        .writeLong(voter.directoryId.leastSignificantBits): Unit
    }

  private def readVoters(cursor: ByteCursor): Vector[CoordinatorCertificateVoter] =
    cursor.readArray {
      CoordinatorCertificateVoter(
        cursor.readInt(),
        VoterDirectoryId(cursor.readLong(), cursor.readLong())
      )
    }

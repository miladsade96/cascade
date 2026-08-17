package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

object MetadataCodec:
  private val FormatVersion: Short = 4

  def encode(metadata: ClusterMetadata): Array[Byte] =
    val writer = ByteWriter()
    writer.writeShort(FormatVersion).writeLong(metadata.version).writeLong(metadata.controllerTerm)
    writer.writeArray(metadata.topics) { topic =>
      writer.writeString(topic.name)
      writer.writeArray(topic.partitions) { partition =>
        writer.writeInt(partition.partition)
        writer.writeInt(partition.leaderId)
        writer.writeInt(partition.leaderEpoch)
        writer.writeArray(partition.replicas)(writer.writeInt)
        writer.writeArray(partition.inSyncReplicas)(writer.writeInt)
        writer.writeArray(partition.addingReplicas)(writer.writeInt)
        writer.writeArray(partition.removingReplicas)(writer.writeInt): Unit
      }
    }
    writer.writeBoolean(metadata.membership.nonEmpty)
    metadata.membership.foreach { membership =>
      writeVoters(writer, membership.currentVoters)
      writeVoters(writer, membership.nextVoters)
    }
    writer.result()

  def decode(bytes: Array[Byte]): ClusterMetadata =
    val cursor = ByteCursor(bytes)
    val format = cursor.readShort()
    if format < 1 || format > FormatVersion then
      throw ProtocolException(s"unsupported cluster metadata format: $format")
    val version = cursor.readLong()
    val controllerTerm = if format >= 2 then cursor.readLong() else 0L
    val topics = cursor.readArray {
      val name = cursor.readString()
      val partitions = cursor.readArray {
        val partition = cursor.readInt()
        val leaderId = cursor.readInt()
        val leaderEpoch = cursor.readInt()
        val replicas = cursor.readArray(cursor.readInt())
        val inSyncReplicas = cursor.readArray(cursor.readInt())
        val addingReplicas = if format >= 3 then cursor.readArray(cursor.readInt()) else Vector.empty
        val removingReplicas = if format >= 3 then cursor.readArray(cursor.readInt()) else Vector.empty
        PartitionMetadata(
          partition,
          leaderId,
          leaderEpoch,
          replicas,
          inSyncReplicas,
          addingReplicas,
          removingReplicas
        )
      }
      TopicMetadata(name, partitions)
    }
    val membership =
      if format >= 4 && cursor.readBoolean() then
        Some(QuorumMembership(readVoters(cursor), readVoters(cursor)))
      else None
    cursor.ensureFullyRead()
    ClusterMetadata(version, topics, controllerTerm, membership)

  private def writeVoters(writer: ByteWriter, voters: Vector[QuorumVoter]): Unit =
    writer.writeArray(voters) { voter =>
      writer.writeInt(voter.id)
      writer.writeString(voter.node.host)
      writer.writeInt(voter.node.port)
      writer.writeLong(voter.directoryId.mostSignificantBits)
      writer.writeLong(voter.directoryId.leastSignificantBits): Unit
    }: Unit

  private def readVoters(cursor: ByteCursor): Vector[QuorumVoter] =
    cursor.readArray {
      val id = cursor.readInt()
      val host = cursor.readString()
      val port = cursor.readInt()
      val directoryId = VoterDirectoryId(cursor.readLong(), cursor.readLong())
      QuorumVoter(ClusterNode(id, host, port), directoryId)
    }

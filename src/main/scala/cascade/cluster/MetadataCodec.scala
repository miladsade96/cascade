package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

object MetadataCodec:
  private val FormatVersion: Short = 2

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
        writer.writeArray(partition.inSyncReplicas)(writer.writeInt): Unit
      }
    }
    writer.result()

  def decode(bytes: Array[Byte]): ClusterMetadata =
    val cursor = ByteCursor(bytes)
    val format = cursor.readShort()
    if format != 1 && format != FormatVersion then
      throw ProtocolException(s"unsupported cluster metadata format: $format")
    val version = cursor.readLong()
    val controllerTerm = if format >= 2 then cursor.readLong() else 0L
    val topics = cursor.readArray {
      val name = cursor.readString()
      val partitions = cursor.readArray {
        PartitionMetadata(
          cursor.readInt(),
          cursor.readInt(),
          cursor.readInt(),
          cursor.readArray(cursor.readInt()),
          cursor.readArray(cursor.readInt())
        )
      }
      TopicMetadata(name, partitions)
    }
    cursor.ensureFullyRead()
    ClusterMetadata(version, topics, controllerTerm)

package cascade.delivery

import cascade.protocol.{ByteCursor, ByteWriter}
import cascade.storage.TopicPartition

final case class ProducerRegistration(
    producerId: Long,
    producerEpoch: Short,
    transactionalId: Option[String],
    transactionTimeoutMillis: Int
)

final case class TransactionRange(topic: String, partition: Int, firstOffset: Long, lastOffset: Long)

final case class PendingOffset(
    groupId: String,
    topic: String,
    partition: Int,
    offset: Long,
    leaderEpoch: Int,
    metadata: Option[String]
)

final case class ActiveTransaction(
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    timeoutMillis: Int,
    startedAtMillis: Long,
    partitions: Vector[TopicPartition],
    ranges: Vector[TransactionRange],
    groups: Vector[String],
    pendingOffsets: Vector[PendingOffset]
)

final case class CompletedTransaction(
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    committed: Boolean,
    offsetsApplied: Boolean,
    ranges: Vector[TransactionRange],
    pendingOffsets: Vector[PendingOffset]
)

final case class DeliveryImage(
    version: Long,
    nextProducerId: Long,
    producers: Vector[ProducerRegistration],
    activeTransactions: Vector[ActiveTransaction],
    completedTransactions: Vector[CompletedTransaction]
):
  lazy val producerById: Map[Long, ProducerRegistration] = producers.map(value => value.producerId -> value).toMap
  lazy val producerByTransactionalId: Map[String, ProducerRegistration] =
    producers.flatMap(value => value.transactionalId.map(_ -> value)).toMap
  lazy val activeByTransactionalId: Map[String, ActiveTransaction] =
    activeTransactions.map(value => value.transactionalId -> value).toMap

object DeliveryImage:
  val Empty: DeliveryImage = DeliveryImage(0L, 1L, Vector.empty, Vector.empty, Vector.empty)

object DeliveryCodec:
  def encode(image: DeliveryImage): Array[Byte] =
    val writer = ByteWriter()
    writer.writeLong(image.version).writeLong(image.nextProducerId)
    writer.writeArray(image.producers) { producer =>
      writer.writeLong(producer.producerId)
      writer.writeShort(producer.producerEpoch)
      writer.writeNullableString(producer.transactionalId)
      writer.writeInt(producer.transactionTimeoutMillis): Unit
    }
    writer.writeArray(image.activeTransactions)(writeActive(writer, _))
    writer.writeArray(image.completedTransactions)(writeCompleted(writer, _))
    writer.result()

  def decode(bytes: Array[Byte]): DeliveryImage =
    val cursor = ByteCursor(bytes)
    val version = cursor.readLong()
    val nextProducerId = cursor.readLong()
    val producers = cursor.readArray {
      ProducerRegistration(cursor.readLong(), cursor.readShort(), cursor.readNullableString(), cursor.readInt())
    }
    val active = cursor.readArray(readActive(cursor))
    val completed = cursor.readArray(readCompleted(cursor))
    cursor.ensureFullyRead()
    DeliveryImage(version, nextProducerId, producers, active, completed)

  private def writeTopicPartition(writer: ByteWriter, value: TopicPartition): Unit =
    writer.writeString(value.topic).writeInt(value.partition): Unit

  private def readTopicPartition(cursor: ByteCursor): TopicPartition =
    TopicPartition(cursor.readString(), cursor.readInt())

  private def writeRange(writer: ByteWriter, value: TransactionRange): Unit =
    writer.writeString(value.topic).writeInt(value.partition).writeLong(value.firstOffset).writeLong(value.lastOffset): Unit

  private def readRange(cursor: ByteCursor): TransactionRange =
    TransactionRange(cursor.readString(), cursor.readInt(), cursor.readLong(), cursor.readLong())

  private def writeOffset(writer: ByteWriter, value: PendingOffset): Unit =
    writer.writeString(value.groupId)
    writer.writeString(value.topic)
    writer.writeInt(value.partition)
    writer.writeLong(value.offset)
    writer.writeInt(value.leaderEpoch)
    writer.writeNullableString(value.metadata): Unit

  private def readOffset(cursor: ByteCursor): PendingOffset =
    PendingOffset(
      cursor.readString(),
      cursor.readString(),
      cursor.readInt(),
      cursor.readLong(),
      cursor.readInt(),
      cursor.readNullableString()
    )

  private def writeActive(writer: ByteWriter, value: ActiveTransaction): Unit =
    writer.writeString(value.transactionalId)
    writer.writeLong(value.producerId)
    writer.writeShort(value.producerEpoch)
    writer.writeInt(value.timeoutMillis)
    writer.writeLong(value.startedAtMillis)
    writer.writeArray(value.partitions)(writeTopicPartition(writer, _))
    writer.writeArray(value.ranges)(writeRange(writer, _))
    writer.writeArray(value.groups)(writer.writeString)
    writer.writeArray(value.pendingOffsets)(writeOffset(writer, _)): Unit

  private def readActive(cursor: ByteCursor): ActiveTransaction =
    ActiveTransaction(
      cursor.readString(),
      cursor.readLong(),
      cursor.readShort(),
      cursor.readInt(),
      cursor.readLong(),
      cursor.readArray(readTopicPartition(cursor)),
      cursor.readArray(readRange(cursor)),
      cursor.readArray(cursor.readString()),
      cursor.readArray(readOffset(cursor))
    )

  private def writeCompleted(writer: ByteWriter, value: CompletedTransaction): Unit =
    writer.writeString(value.transactionalId)
    writer.writeLong(value.producerId)
    writer.writeShort(value.producerEpoch)
    writer.writeBoolean(value.committed)
    writer.writeBoolean(value.offsetsApplied)
    writer.writeArray(value.ranges)(writeRange(writer, _))
    writer.writeArray(value.pendingOffsets)(writeOffset(writer, _)): Unit

  private def readCompleted(cursor: ByteCursor): CompletedTransaction =
    CompletedTransaction(
      cursor.readString(),
      cursor.readLong(),
      cursor.readShort(),
      cursor.readBoolean(),
      cursor.readBoolean(),
      cursor.readArray(readRange(cursor)),
      cursor.readArray(readOffset(cursor))
    )

package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter}
import cascade.storage.{PartitionCleanupDecision, PartitionCleanupPlan}

private[cascade] final case class ReplicaCleanupRequest(
    topic: String,
    partition: Int,
    leaderId: Int,
    decision: PartitionCleanupDecision
):
  require(topic.nonEmpty, "cleanup topic must not be empty")
  require(partition >= 0, "cleanup partition must be non-negative")
  require(leaderId >= 0, "cleanup leader ID must be non-negative")

private[cascade] object ReplicaCleanupCodec:
  def encode(request: ReplicaCleanupRequest): Array[Byte] =
    ByteWriter()
      .writeString(request.topic)
      .writeInt(request.partition)
      .writeInt(request.leaderId)
      .writeInt(request.decision.leaderEpoch)
      .writeLong(request.decision.generation)
      .writeLong(request.decision.plan.deleteBeforeOffset)
      .writeLong(request.decision.plan.compactBeforeOffset)
      .writeLong(request.decision.plan.tombstoneCutoffMillis)
      .result()

  def decode(cursor: ByteCursor): ReplicaCleanupRequest =
    val request = ReplicaCleanupRequest(
      cursor.readString(),
      cursor.readInt(),
      cursor.readInt(),
      PartitionCleanupDecision(
        cursor.readInt(),
        cursor.readLong(),
        PartitionCleanupPlan(cursor.readLong(), cursor.readLong(), cursor.readLong())
      )
    )
    cursor.ensureFullyRead()
    request

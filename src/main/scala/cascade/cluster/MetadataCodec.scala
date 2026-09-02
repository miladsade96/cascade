package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.storage.{CleanupPolicy, TopicLifecyclePolicy}

object MetadataCodec:
  val MinimumReadableFormat: Short = 1
  val CurrentFormat: Short = 11

  def encode(metadata: ClusterMetadata): Array[Byte] =
    encode(metadata, minimumRequiredFormat(metadata))

  def encode(metadata: ClusterMetadata, format: Short): Array[Byte] =
    if format < MinimumReadableFormat || format > CurrentFormat then
      throw ProtocolException(s"unsupported cluster metadata format: $format")
    val required = minimumRequiredFormat(metadata)
    if format < required then
      throw ProtocolException(s"cluster metadata requires format $required but negotiated format is $format")
    val writer = ByteWriter()
    writer.writeShort(format).writeLong(metadata.version)
    if format >= 2 then writer.writeLong(metadata.controllerTerm)
    writer.writeArray(metadata.topics) { topic =>
      writer.writeString(topic.name)
      writer.writeArray(topic.partitions) { partition =>
        writer.writeInt(partition.partition)
        writer.writeInt(partition.leaderId)
        writer.writeInt(partition.leaderEpoch)
        writer.writeArray(partition.replicas)(writer.writeInt)
        writer.writeArray(partition.inSyncReplicas)(writer.writeInt)
        if format >= 3 then
          writer.writeArray(partition.addingReplicas)(writer.writeInt)
          writer.writeArray(partition.removingReplicas)(writer.writeInt): Unit
      }
      if format >= 6 then
        writer.writeBoolean(topic.lifecyclePolicy.nonEmpty)
        topic.lifecyclePolicy.foreach { policy =>
          writer.writeByte(cleanupPolicyCode(policy.cleanupPolicy))
          writer.writeLong(policy.retentionMillis)
          writer.writeLong(policy.retentionBytes): Unit
        }
    }
    if format >= 4 then
      writer.writeBoolean(metadata.membership.nonEmpty)
      metadata.membership.foreach { membership =>
        writeVoters(writer, membership.currentVoters)
        writeVoters(writer, membership.nextVoters)
      }
    if format >= 5 then
      writer.writeLong(metadata.coordinator.version)
      writer.writeLong(metadata.coordinator.ownerTerm)
      writer.writeByteArray(metadata.coordinator.groupState.toArray)
      writer.writeByteArray(metadata.coordinator.deliveryState.toArray)
    if format >= 7 then
      writer.writeArray(metadata.featureLevels.toVector.sortBy(_._1)) { case (name, level) =>
        writer.writeString(name).writeShort(level): Unit
      }
    if format >= 8 then writer.writeArray(metadata.unavailableBrokerIds.toVector.sorted)(writer.writeInt)
    if format >= 9 then writer.writeArray(metadata.coordinator.shardVersions)(writer.writeLong)
    writer.result()

  def minimumRequiredFormat(metadata: ClusterMetadata): Short =
    if metadata.featureLevels.contains(ClusterFeature.ShardObjectStorage) then 11
    else if metadata.featureLevels.contains(ClusterFeature.IncrementalCoordinator) then 10
    else if metadata.coordinator.shardVersions.nonEmpty || metadata.featureLevels.contains(ClusterFeature.CoordinatorDeltas) then 9
    else if metadata.unavailableBrokerIds.nonEmpty || metadata.featureLevels.contains(ClusterFeature.CoordinatorFailover) then 8
    else if metadata.featureLevels.nonEmpty then 7
    else if metadata.topics.exists(_.lifecyclePolicy.nonEmpty) then 6
    else if metadata.coordinator != CoordinatorMetadata.Empty then 5
    else if metadata.membership.nonEmpty then 4
    else if metadata.topics.exists(_.partitions.exists(_.isReassigning)) then 3
    else if metadata.controllerTerm != 0L then 2
    else 1

  def decode(bytes: Array[Byte]): ClusterMetadata =
    val cursor = ByteCursor(bytes)
    val format = cursor.readShort()
    if format < MinimumReadableFormat || format > CurrentFormat then
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
      val policy =
        if format >= 6 && cursor.readBoolean() then
          Some(TopicLifecyclePolicy(readCleanupPolicy(cursor.readByte()), cursor.readLong(), cursor.readLong()))
        else None
      TopicMetadata(name, partitions, policy)
    }
    val membership =
      if format >= 4 && cursor.readBoolean() then
        Some(QuorumMembership(readVoters(cursor), readVoters(cursor)))
      else None
    val coordinator =
      if format >= 5 then
        CoordinatorMetadata(
          cursor.readLong(),
          cursor.readLong(),
          cursor.readByteArray().toVector,
          cursor.readByteArray().toVector
        )
      else CoordinatorMetadata.Empty
    val featureLevels =
      if format >= 7 then cursor.readArray((cursor.readString(), cursor.readShort())).toMap
      else Map.empty[String, Short]
    val unavailableBrokerIds =
      if format >= 8 then cursor.readArray(cursor.readInt()).toSet
      else Set.empty[Int]
    val shardVersions = if format >= 9 then cursor.readArray(cursor.readLong()) else Vector.empty
    cursor.ensureFullyRead()
    ClusterMetadata(version, topics, controllerTerm, membership, coordinator.copy(shardVersions = shardVersions), featureLevels, unavailableBrokerIds)

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

  private def cleanupPolicyCode(policy: CleanupPolicy): Int = policy match
    case CleanupPolicy.Delete        => 0
    case CleanupPolicy.Compact       => 1
    case CleanupPolicy.CompactDelete => 2

  private def readCleanupPolicy(value: Byte): CleanupPolicy = value.toInt match
    case 0 => CleanupPolicy.Delete
    case 1 => CleanupPolicy.Compact
    case 2 => CleanupPolicy.CompactDelete
    case other => throw ProtocolException(s"unsupported topic cleanup policy: $other")

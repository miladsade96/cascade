package cascade.group

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

private[cascade] enum GroupStatus(val id: Byte):
  case Empty extends GroupStatus(0)
  case PreparingRebalance extends GroupStatus(1)
  case CompletingRebalance extends GroupStatus(2)
  case Stable extends GroupStatus(3)

private[cascade] object GroupStatus:
  def fromId(id: Byte): GroupStatus =
    GroupStatus.values.find(_.id == id).getOrElse(throw ProtocolException(s"unsupported group status: $id"))

private[cascade] final case class StoredProtocol(name: String, metadata: Vector[Byte])
private[cascade] final case class StoredMember(
    memberId: String,
    groupInstanceId: Option[String],
    sessionTimeoutMillis: Int,
    rebalanceTimeoutMillis: Int,
    protocols: Vector[StoredProtocol],
    clientId: String,
    lastHeartbeatMillis: Long,
    assignment: Vector[Byte]
)
private[cascade] final case class StoredGroup(
    groupId: String,
    status: GroupStatus,
    generationId: Int,
    leaderId: String,
    protocolType: String,
    protocolName: String,
    rebalanceDeadlineMillis: Long,
    members: Vector[StoredMember],
    joined: Vector[String],
    pendingMemberIds: Vector[(String, Long)]
)
private[cascade] final case class GroupImage(
    version: Long,
    groups: Vector[StoredGroup],
    offsets: Vector[OffsetCommitValue],
    consumerGroups: Vector[StoredConsumerGroup] = Vector.empty
)

private[cascade] object GroupImage:
  val Empty: GroupImage = GroupImage(0L, Vector.empty, Vector.empty, Vector.empty)

private[cascade] object GroupCodec:
  private val FormatVersion: Short = 2

  def encode(image: GroupImage): Array[Byte] =
    val format: Short = if image.consumerGroups.isEmpty then 1 else FormatVersion
    val writer = ByteWriter().writeShort(format).writeLong(image.version)
    writer.writeArray(image.groups) { group =>
      writer.writeString(group.groupId)
      writer.writeByte(group.status.id)
      writer.writeInt(group.generationId)
      writer.writeString(group.leaderId)
      writer.writeString(group.protocolType)
      writer.writeString(group.protocolName)
      writer.writeLong(group.rebalanceDeadlineMillis)
      writer.writeArray(group.members) { member =>
        writer.writeString(member.memberId)
        writer.writeNullableString(member.groupInstanceId)
        writer.writeInt(member.sessionTimeoutMillis)
        writer.writeInt(member.rebalanceTimeoutMillis)
        writer.writeArray(member.protocols) { protocol =>
          writer.writeString(protocol.name).writeByteArray(protocol.metadata.toArray): Unit
        }
        writer.writeString(member.clientId)
        writer.writeLong(member.lastHeartbeatMillis)
        writer.writeByteArray(member.assignment.toArray): Unit
      }
      writer.writeArray(group.joined)(writer.writeString)
      writer.writeArray(group.pendingMemberIds) { case (memberId, deadlineMillis) =>
        writer.writeString(memberId).writeLong(deadlineMillis): Unit
      }: Unit
    }
    writer.writeArray(image.offsets) { entry =>
      writer.writeString(entry.key.groupId)
      writer.writeString(entry.key.topic)
      writer.writeInt(entry.key.partition)
      writer.writeLong(entry.value.offset)
      writer.writeInt(entry.value.leaderEpoch)
      writer.writeNullableString(entry.value.metadata)
      writer.writeLong(entry.value.committedAtMillis): Unit
    }
    if format >= 2 then
      writer.writeArray(image.consumerGroups) { group =>
        writer.writeString(group.groupId).writeInt(group.groupEpoch)
        writer.writeArray(group.members) { member =>
          writer.writeString(member.memberId)
          writer.writeNullableString(member.instanceId)
          writer.writeNullableString(member.rackId)
          writer.writeInt(member.rebalanceTimeoutMillis)
          writer.writeArray(member.subscriptions)(writer.writeString)
          writer.writeString(member.serverAssignor)
          writer.writeInt(member.memberEpoch)
          writer.writeLong(member.lastHeartbeatMillis)
          writer.writeArray(member.assignment) { topic =>
            writer.writeUuid(topic.topicId.mostSignificantBits, topic.topicId.leastSignificantBits)
            writer.writeArray(topic.partitions)(writer.writeInt): Unit
          }: Unit
        }: Unit
      }
    writer.result()

  def decode(bytes: Array[Byte]): GroupImage =
    val cursor = ByteCursor(bytes)
    val format = cursor.readShort()
    if format < 1 || format > FormatVersion then throw ProtocolException(s"unsupported group-state format: $format")
    val version = cursor.readLong()
    val groups = cursor.readArray {
      val groupId = cursor.readString()
      val status = GroupStatus.fromId(cursor.readByte())
      val generationId = cursor.readInt()
      val leaderId = cursor.readString()
      val protocolType = cursor.readString()
      val protocolName = cursor.readString()
      val rebalanceDeadlineMillis = cursor.readLong()
      val members = cursor.readArray {
        StoredMember(
          cursor.readString(),
          cursor.readNullableString(),
          cursor.readInt(),
          cursor.readInt(),
          cursor.readArray(StoredProtocol(cursor.readString(), cursor.readByteArray().toVector)),
          cursor.readString(),
          cursor.readLong(),
          cursor.readByteArray().toVector
        )
      }
      val joined = cursor.readArray(cursor.readString())
      val pending = cursor.readArray((cursor.readString(), cursor.readLong()))
      StoredGroup(
        groupId,
        status,
        generationId,
        leaderId,
        protocolType,
        protocolName,
        rebalanceDeadlineMillis,
        members,
        joined,
        pending
      )
    }
    val offsets = cursor.readArray {
      val key = GroupOffsetKey(cursor.readString(), cursor.readString(), cursor.readInt())
      val value = CommittedOffset(cursor.readLong(), cursor.readInt(), cursor.readNullableString(), cursor.readLong())
      OffsetCommitValue(key, value)
    }
    val consumerGroups =
      if format >= 2 then cursor.readArray {
        val groupId = cursor.readString()
        val groupEpoch = cursor.readInt()
        val members = cursor.readArray {
          StoredConsumerMember(
            cursor.readString(),
            cursor.readNullableString(),
            cursor.readNullableString(),
            cursor.readInt(),
            cursor.readArray(cursor.readString()),
            cursor.readString(),
            cursor.readInt(),
            cursor.readLong(),
            cursor.readArray {
              val (high, low) = cursor.readUuid()
              ConsumerTopicPartitions(ConsumerTopicId(high, low), cursor.readArray(cursor.readInt()))
            }
          )
        }
        StoredConsumerGroup(groupId, groupEpoch, members)
      }
      else Vector.empty
    cursor.ensureFullyRead()
    GroupImage(version, groups, offsets, consumerGroups)

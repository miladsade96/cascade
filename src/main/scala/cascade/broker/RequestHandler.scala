package cascade.broker

import cascade.cluster.*
import cascade.delivery.*
import cascade.group.*
import cascade.protocol.*
import cascade.storage.TopicRegistry

final class RequestHandler(
    config: BrokerConfig,
    registry: TopicRegistry,
    groupCoordinator: GroupCoordinator,
    clusterManager: ClusterManager,
    replicationManager: ReplicationManager,
    deliveryCoordinator: DeliveryCoordinator,
    advertisedPort: Int
):
  def handle(frame: Array[Byte]): Option[Array[Byte]] =
    val (header, body) = RequestHeader.decode(frame)
    if InternalApi.contains(header.apiKey) then
      if !header.clientId.contains("cascade-peer") then throw ProtocolException("internal API requires a peer client")
      val response = header.apiKey match
        case InternalApi.ReplicaAppend | InternalApi.ReplicaCommit | InternalApi.ReplicaCatchUp |
            InternalApi.ReplicaReset | InternalApi.ReplicaRecoveryComplete =>
          replicationManager.handleInternal(header.apiKey, body)
        case _ => clusterManager.handleInternal(header.apiKey, body)
      return Some(ResponseFrame.encode(header, response))
    if !Compatibility.accepts(header.apiKey, header.apiVersion) then
      if header.apiKey == ApiKey.ApiVersions then
        return Some(ResponseFrame.encode(header, unsupportedApiVersions()))
      throw ProtocolException(s"unsupported API ${header.apiKey} version ${header.apiVersion}")

    val response = header.apiKey match
      case ApiKey.ApiVersions  => apiVersions(header.apiVersion, body)
      case ApiKey.Metadata     => metadata(body)
      case ApiKey.OffsetCommit => offsetCommit(body)
      case ApiKey.OffsetFetch  => offsetFetch(body)
      case ApiKey.FindCoordinator => findCoordinator(body)
      case ApiKey.JoinGroup    => joinGroup(header, body)
      case ApiKey.Heartbeat    => heartbeat(body)
      case ApiKey.LeaveGroup   => leaveGroup(body)
      case ApiKey.SyncGroup    => syncGroup(body)
      case ApiKey.CreateTopics => createTopics(body)
      case ApiKey.InitProducerId => initProducerId(body)
      case ApiKey.AddPartitionsToTxn => addPartitionsToTxn(body)
      case ApiKey.AddOffsetsToTxn => addOffsetsToTxn(body)
      case ApiKey.EndTxn => endTxn(body)
      case ApiKey.TxnOffsetCommit => txnOffsetCommit(body)
      case ApiKey.Produce      => produce(body)
      case ApiKey.Fetch        => fetch(body)
      case ApiKey.ListOffsets  => listOffsets(body)
      case other               => throw ProtocolException(s"unsupported API key: $other")
    response.map(ResponseFrame.encode(header, _))

  private def unsupportedApiVersions(): Array[Byte] =
    val writer = ByteWriter()
    writer.writeShort(Errors.UnsupportedVersion)
    writer.writeArray(Compatibility.supported) { api =>
      writer.writeShort(api.apiKey).writeShort(api.minVersion).writeShort(api.maxVersion): Unit
    }
    writer.result()

  private def apiVersions(version: Short, cursor: ByteCursor): Option[Array[Byte]] =
    if version >= 3 then
      cursor.readCompactString()
      cursor.readCompactString()
      cursor.skipTaggedFields()
    cursor.ensureFullyRead()

    val writer = ByteWriter()
    writer.writeShort(Errors.None)
    if version >= 3 then
      writer.writeCompactArray(Compatibility.supported) { api =>
        writer.writeShort(api.apiKey).writeShort(api.minVersion).writeShort(api.maxVersion).writeEmptyTaggedFields(): Unit
      }
    else
      writer.writeArray(Compatibility.supported) { api =>
        writer.writeShort(api.apiKey).writeShort(api.minVersion).writeShort(api.maxVersion): Unit
      }
    if version >= 1 then writer.writeInt(0)
    if version >= 3 then writer.writeEmptyTaggedFields()
    Some(writer.result())

  private def findCoordinator(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readString()
    val coordinatorType = cursor.readByte()
    cursor.ensureFullyRead()
    val supported = coordinatorType == 0.toByte || coordinatorType == 1.toByte
    val coordinator = if clusterManager.isEnabled then clusterManager.controllerNode else
      ClusterNode(config.nodeId, config.advertisedHost, advertisedPort)
    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeShort(if supported then Errors.None else Errors.CoordinatorNotAvailable)
    writer.writeNullableString(if supported then None else Some("unsupported coordinator type"))
    writer.writeInt(if supported then coordinator.id else -1)
    writer.writeString(if supported then coordinator.host else "")
    writer.writeInt(if supported then coordinator.port else -1)
    Some(writer.result())

  private def joinGroup(header: RequestHeader, cursor: ByteCursor): Option[Array[Byte]] =
    val groupId = cursor.readString()
    val sessionTimeout = cursor.readInt()
    val rebalanceTimeout = cursor.readInt()
    val memberId = cursor.readString()
    val groupInstanceId = cursor.readNullableString()
    val protocolType = cursor.readString()
    val protocols = cursor.readArray(GroupProtocol(cursor.readString(), cursor.readByteArray()))
    cursor.ensureFullyRead()
    val result = groupCoordinator.join(
      JoinGroupCommand(
        groupId,
        sessionTimeout,
        rebalanceTimeout,
        memberId,
        groupInstanceId,
        protocolType,
        protocols,
        header.clientId.getOrElse("")
      )
    )
    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeShort(result.errorCode)
    writer.writeInt(result.generationId)
    writer.writeString(result.protocolName)
    writer.writeString(result.leaderId)
    writer.writeString(result.memberId)
    writer.writeArray(result.members) { member =>
      writer.writeString(member.memberId)
      writer.writeNullableString(member.groupInstanceId)
      writer.writeByteArray(member.metadata): Unit
    }
    Some(writer.result())

  private def heartbeat(cursor: ByteCursor): Option[Array[Byte]] =
    val groupId = cursor.readString()
    val generationId = cursor.readInt()
    val memberId = cursor.readString()
    cursor.readNullableString()
    cursor.ensureFullyRead()
    Some(ByteWriter().writeInt(0).writeShort(groupCoordinator.heartbeat(groupId, generationId, memberId)).result())

  private def leaveGroup(cursor: ByteCursor): Option[Array[Byte]] =
    val groupId = cursor.readString()
    val memberId = cursor.readString()
    cursor.ensureFullyRead()
    Some(ByteWriter().writeInt(0).writeShort(groupCoordinator.leave(groupId, memberId)).result())

  private def syncGroup(cursor: ByteCursor): Option[Array[Byte]] =
    val groupId = cursor.readString()
    val generationId = cursor.readInt()
    val memberId = cursor.readString()
    cursor.readNullableString()
    val assignments = cursor.readArray((cursor.readString(), cursor.readByteArray()))
    cursor.ensureFullyRead()
    val result = groupCoordinator.sync(groupId, generationId, memberId, assignments)
    Some(ByteWriter().writeInt(0).writeShort(result.errorCode).writeByteArray(result.assignment).result())

  private def offsetCommit(cursor: ByteCursor): Option[Array[Byte]] =
    final case class RequestedPartition(index: Int, value: OffsetCommitValue, exists: Boolean)
    val groupId = cursor.readString()
    val generationId = cursor.readInt()
    val memberId = cursor.readString()
    cursor.readNullableString()
    val committedAt = System.currentTimeMillis()
    val requests = cursor.readArray {
      val topic = cursor.readString()
      val partitions = cursor.readArray {
        val index = cursor.readInt()
        val offset = cursor.readLong()
        val leaderEpoch = cursor.readInt()
        val metadata = cursor.readNullableString()
        val value = OffsetCommitValue(
          GroupOffsetKey(groupId, topic, index),
          CommittedOffset(offset, leaderEpoch, metadata, committedAt)
        )
        RequestedPartition(index, value, registry.partition(topic, index).nonEmpty)
      }
      (topic, partitions)
    }
    cursor.ensureFullyRead()
    val validValues = requests.flatMap(_._2).filter(_.exists).map(_.value)
    val groupError = groupCoordinator.commitOffsets(groupId, generationId, memberId, validValues)
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(requests) { case (topic, partitions) =>
      writer.writeString(topic)
      writer.writeArray(partitions) { partition =>
        val error = if partition.exists then groupError else Errors.UnknownTopicOrPartition
        writer.writeInt(partition.index).writeShort(error): Unit
      }
    }
    Some(writer.result())

  private def offsetFetch(cursor: ByteCursor): Option[Array[Byte]] =
    val groupId = cursor.readString()
    val requested = cursor.readNullableArray {
      val topic = cursor.readString()
      (topic, cursor.readArray(cursor.readInt()))
    }
    cursor.ensureFullyRead()
    val offsets = requested match
      case Some(topics) => topics.map { case (topic, partitions) =>
          val values = partitions.map { partition =>
            val key = GroupOffsetKey(groupId, topic, partition)
            (partition, groupCoordinator.fetchOffset(key))
          }
          (topic, values)
        }
      case None =>
        groupCoordinator.allOffsets(groupId)
          .groupBy(_._1.topic)
          .toVector
          .sortBy(_._1)
          .map { case (topic, values) =>
            (topic, values.sortBy(_._1.partition).map { case (key, value) => (key.partition, Some(value)) })
          }
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(offsets) { case (topic, partitions) =>
      writer.writeString(topic)
      writer.writeArray(partitions) { case (partition, committed) =>
        writer.writeInt(partition)
        writer.writeLong(committed.map(_.offset).getOrElse(-1L))
        writer.writeInt(committed.map(_.leaderEpoch).getOrElse(-1))
        writer.writeNullableString(committed.flatMap(_.metadata))
        writer.writeShort(Errors.None): Unit
      }
    }
    writer.writeShort(Errors.None)
    Some(writer.result())

  private def metadata(cursor: ByteCursor): Option[Array[Byte]] =
    val requestedTopics = cursor.readNullableArray(cursor.readString())
    val allowAutoCreation = cursor.readBoolean()
    cursor.ensureFullyRead()

    requestedTopics.foreach { names =>
      if config.autoCreateTopics && allowAutoCreation then
        names.foreach { name =>
          if clusterManager.isEnabled then
            if clusterManager.topic(name).isEmpty then
              clusterManager.createTopic(name, 1, config.defaultReplicationFactor): Unit
          else registry.getOrCreate(name): Unit
        }
    }
    val topicNames = requestedTopics.getOrElse(clusterManager.topicNames)
    val brokers = clusterManager.clusterNodes
    val writer = ByteWriter()
    writer.writeInt(0) // throttle_time_ms
    writer.writeArray(brokers) { broker =>
      writer.writeInt(broker.id)
      writer.writeString(broker.host)
      writer.writeInt(broker.port)
      writer.writeNullableString(None)
    }
    writer.writeNullableString(Some("cascade-cluster"))
    writer.writeInt(if clusterManager.isEnabled then config.controllerId else config.nodeId)
    writer.writeArray(topicNames) { topic =>
      val clusterTopic = clusterManager.topic(topic)
      val localPartitions = registry.partitions(topic)
      if clusterManager.isEnabled then clusterTopic match
        case None =>
          writer.writeShort(Errors.UnknownTopicOrPartition).writeString(topic).writeBoolean(false)
          writer.writeArray(Vector.empty[Int])(_ => ())
        case Some(metadata) =>
          writer.writeShort(Errors.None).writeString(topic).writeBoolean(topic.startsWith("__"))
          writer.writeArray(metadata.partitions) { partition =>
            writer.writeShort(if partition.leaderId < 0 then Errors.LeaderNotAvailable else Errors.None)
            writer.writeInt(partition.partition)
            writer.writeInt(partition.leaderId)
            writer.writeArray(partition.replicas)(writer.writeInt)
            writer.writeArray(partition.inSyncReplicas)(writer.writeInt)
          }
      else localPartitions match
        case None =>
          writer.writeShort(Errors.UnknownTopicOrPartition).writeString(topic).writeBoolean(false)
          writer.writeArray(Vector.empty[Int])(_ => ())
        case Some(partitions) =>
          writer.writeShort(Errors.None).writeString(topic).writeBoolean(topic.startsWith("__"))
          writer.writeArray(partitions.indices) { index =>
            writer.writeShort(Errors.None).writeInt(index).writeInt(config.nodeId)
            writer.writeArray(Vector(config.nodeId))(writer.writeInt)
            writer.writeArray(Vector(config.nodeId))(writer.writeInt): Unit
          }
    }
    Some(writer.result())

  private def createTopics(cursor: ByteCursor): Option[Array[Byte]] =
    final case class RequestedTopic(name: String, partitions: Int, replicationFactor: Short)
    val topics = cursor.readArray {
      val topic = RequestedTopic(cursor.readString(), cursor.readInt(), cursor.readShort())
      cursor.readArray {
        cursor.readInt()
        cursor.readArray(cursor.readInt())
      }
      cursor.readArray {
        cursor.readString()
        cursor.readNullableString()
      }
      topic
    }
    cursor.readInt() // timeout_ms
    val validateOnly = cursor.readBoolean()
    cursor.ensureFullyRead()

    val results = topics.map { topic =>
      val replicationFactor = topic.replicationFactor.toInt
      val result =
        if validateOnly then clusterManager.validateTopic(topic.name, topic.partitions, replicationFactor)
        else clusterManager.createTopic(topic.name, topic.partitions, replicationFactor)
      (topic.name, result.errorCode, result.message)
    }
    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeArray(results) { case (name, error, message) =>
      writer.writeString(name).writeShort(error).writeNullableString(message): Unit
    }
    Some(writer.result())

  private def produce(cursor: ByteCursor): Option[Array[Byte]] =
    val transactionalId = cursor.readNullableString()
    val acknowledgements = cursor.readShort()
    val timeoutMillis = cursor.readInt()
    val requests = cursor.readArray {
      val topic = cursor.readString()
      val partitions = cursor.readArray {
        val index = cursor.readInt()
        val records = cursor.readNullableBytes()
        (index, records)
      }
      (topic, partitions)
    }
    cursor.ensureFullyRead()

    val results = requests.map { case (topic, partitions) =>
      if config.autoCreateTopics then
        if clusterManager.isEnabled then
          if clusterManager.topic(topic).isEmpty then
            clusterManager.createTopic(topic, 1, config.defaultReplicationFactor): Unit
        else registry.getOrCreate(topic): Unit
      val partitionResults = partitions.map { case (index, records) =>
        records match
          case Some(batch) =>
            val result = deliveryCoordinator.append(
              transactionalId,
              topic,
              index,
              batch,
              acknowledgements,
              timeoutMillis,
              replicationManager
            )
            (index, result.errorCode, result.baseOffset)
          case None => (index, Errors.InvalidRequest, -1L)
      }
      (topic, partitionResults)
    }

    if acknowledgements == 0 then None
    else
      val writer = ByteWriter()
      writer.writeArray(results) { case (topic, partitions) =>
        writer.writeString(topic)
        writer.writeArray(partitions) { case (index, error, baseOffset) =>
          writer.writeInt(index).writeShort(error).writeLong(baseOffset).writeLong(-1L): Unit
        }
      }
      writer.writeInt(0)
      Some(writer.result())

  private def fetch(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readInt() // replica_id
    cursor.readInt() // max_wait_ms
    cursor.readInt() // min_bytes
    val requestMaxBytes = cursor.readInt()
    val readCommitted = cursor.readByte() == 1.toByte
    val requests = cursor.readArray {
      val topic = cursor.readString()
      val partitions = cursor.readArray {
        val index = cursor.readInt()
        val offset = cursor.readLong()
        cursor.readLong() // log_start_offset
        val maxBytes = cursor.readInt()
        (index, offset, maxBytes)
      }
      (topic, partitions)
    }
    cursor.ensureFullyRead()

    var responseBudget = math.max(0, requestMaxBytes)
    val results = requests.map { case (topic, partitions) =>
      val values = partitions.map { case (index, offset, partitionMaxBytes) =>
        val partitionMetadata = clusterManager.partition(topic, index)
        if clusterManager.isEnabled && partitionMetadata.isEmpty then
          (index, Errors.UnknownTopicOrPartition, None)
        else if clusterManager.isEnabled && partitionMetadata.exists(_.leaderId != config.nodeId) then
          (index, Errors.NotLeaderOrFollower, None)
        else
          registry.partition(topic, index) match
            case None => (index, Errors.UnknownTopicOrPartition, None)
            case Some(log) =>
              val budget = math.min(math.max(0, partitionMaxBytes), responseBudget)
              val lastStableOffset =
                if readCommitted then deliveryCoordinator.lastStableOffset(topic, index, log.highWatermark)
                else log.highWatermark
              val result = log.fetch(
                offset,
                budget,
                lastStableOffset,
                batch => !readCommitted || deliveryCoordinator.visible(topic, index, batch)
              )
              responseBudget = math.max(0, responseBudget - result.records.length)
              (index, Errors.None, Some(result))
      }
      (topic, values)
    }

    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeArray(results) { case (topic, partitions) =>
      writer.writeString(topic)
      writer.writeArray(partitions) { case (index, error, result) =>
        writer.writeInt(index).writeShort(error)
        writer.writeLong(result.map(_.highWatermark).getOrElse(-1L))
        writer.writeLong(result.map(_.lastStableOffset).getOrElse(-1L))
        writer.writeLong(result.map(_.logStartOffset).getOrElse(-1L))
        writer.writeInt(-1) // aborted_transactions: null
        writer.writeNullableBytes(result.map(_.records))
      }
    }
    Some(writer.result())

  private def listOffsets(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readInt() // replica_id
    val readCommitted = cursor.readByte() == 1.toByte
    val requests = cursor.readArray {
      val topic = cursor.readString()
      val partitions = cursor.readArray((cursor.readInt(), cursor.readLong()))
      (topic, partitions)
    }
    cursor.ensureFullyRead()

    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeArray(requests) { case (topic, partitions) =>
      writer.writeString(topic)
      writer.writeArray(partitions) { case (index, timestamp) =>
        val partitionMetadata = clusterManager.partition(topic, index)
        if clusterManager.isEnabled && partitionMetadata.isEmpty then
          writer.writeInt(index).writeShort(Errors.UnknownTopicOrPartition).writeLong(-1L).writeLong(-1L): Unit
        else if clusterManager.isEnabled && partitionMetadata.exists(_.leaderId != config.nodeId) then
          writer.writeInt(index).writeShort(Errors.NotLeaderOrFollower).writeLong(-1L).writeLong(-1L): Unit
        else
          registry.partition(topic, index) match
            case Some(log) =>
              val offset =
                if timestamp == -1L then
                  deliveryCoordinator.latestOffset(topic, index, log.highWatermark, readCommitted)
                else log.offsetForTimestamp(timestamp)
              writer.writeInt(index).writeShort(Errors.None).writeLong(-1L).writeLong(offset): Unit
            case None =>
              writer.writeInt(index).writeShort(Errors.UnknownTopicOrPartition).writeLong(-1L).writeLong(-1L): Unit
      }
    }
    Some(writer.result())

  private def initProducerId(cursor: ByteCursor): Option[Array[Byte]] =
    val transactionalId = cursor.readNullableString()
    val timeoutMillis = cursor.readInt()
    cursor.ensureFullyRead()
    val result = deliveryCoordinator.initProducerId(transactionalId, timeoutMillis)
    Some(
      ByteWriter()
        .writeInt(0)
        .writeShort(result.errorCode)
        .writeLong(result.producerId)
        .writeShort(result.producerEpoch)
        .result()
    )

  private def addPartitionsToTxn(cursor: ByteCursor): Option[Array[Byte]] =
    val transactionalId = cursor.readString()
    val producerId = cursor.readLong()
    val producerEpoch = cursor.readShort()
    val requested = cursor.readArray {
      val topic = cursor.readString()
      (topic, cursor.readArray(cursor.readInt()))
    }
    cursor.ensureFullyRead()
    val valid = requested.flatMap { case (topic, partitions) =>
      partitions.filter(partitionExists(topic, _)).map(index => cascade.storage.TopicPartition(topic, index))
    }
    val transactionError = deliveryCoordinator.addPartitions(transactionalId, producerId, producerEpoch, valid)
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(requested) { case (topic, partitions) =>
      writer.writeString(topic)
      writer.writeArray(partitions) { index =>
        val error = if partitionExists(topic, index) then transactionError else Errors.UnknownTopicOrPartition
        writer.writeInt(index).writeShort(error): Unit
      }
    }
    Some(writer.result())

  private def addOffsetsToTxn(cursor: ByteCursor): Option[Array[Byte]] =
    val transactionalId = cursor.readString()
    val producerId = cursor.readLong()
    val producerEpoch = cursor.readShort()
    val groupId = cursor.readString()
    cursor.ensureFullyRead()
    val error = deliveryCoordinator.addOffsets(transactionalId, producerId, producerEpoch, groupId)
    Some(ByteWriter().writeInt(0).writeShort(error).result())

  private def endTxn(cursor: ByteCursor): Option[Array[Byte]] =
    val transactionalId = cursor.readString()
    val producerId = cursor.readLong()
    val producerEpoch = cursor.readShort()
    val committed = cursor.readBoolean()
    cursor.ensureFullyRead()
    val error = deliveryCoordinator.endTransaction(transactionalId, producerId, producerEpoch, committed)
    Some(ByteWriter().writeInt(0).writeShort(error).result())

  private def txnOffsetCommit(cursor: ByteCursor): Option[Array[Byte]] =
    final case class RequestedOffset(index: Int, value: PendingOffset, exists: Boolean)
    val transactionalId = cursor.readString()
    val groupId = cursor.readString()
    val producerId = cursor.readLong()
    val producerEpoch = cursor.readShort()
    val requested = cursor.readArray {
      val topic = cursor.readString()
      val offsets = cursor.readArray {
        val index = cursor.readInt()
        val offset = cursor.readLong()
        val leaderEpoch = cursor.readInt()
        val metadata = cursor.readNullableString()
        RequestedOffset(
          index,
          PendingOffset(groupId, topic, index, offset, leaderEpoch, metadata),
          partitionExists(topic, index)
        )
      }
      (topic, offsets)
    }
    cursor.ensureFullyRead()
    val values = requested.flatMap(_._2).filter(_.exists).map(_.value)
    val transactionError =
      deliveryCoordinator.stageOffsets(transactionalId, producerId, producerEpoch, groupId, values)
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(requested) { case (topic, offsets) =>
      writer.writeString(topic)
      writer.writeArray(offsets) { offset =>
        val error = if offset.exists then transactionError else Errors.UnknownTopicOrPartition
        writer.writeInt(offset.index).writeShort(error): Unit
      }
    }
    Some(writer.result())

  private def partitionExists(topic: String, partition: Int): Boolean =
    if clusterManager.isEnabled then clusterManager.partition(topic, partition).nonEmpty
    else registry.partition(topic, partition).nonEmpty

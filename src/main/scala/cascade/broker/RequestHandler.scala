package cascade.broker

import cascade.protocol.*
import cascade.storage.{CreateTopicResult, TopicRegistry}

final class RequestHandler(config: BrokerConfig, registry: TopicRegistry, advertisedPort: Int):
  def handle(frame: Array[Byte]): Option[Array[Byte]] =
    val (header, body) = RequestHeader.decode(frame)
    if !Compatibility.accepts(header.apiKey, header.apiVersion) then
      if header.apiKey == ApiKey.ApiVersions then
        return Some(ResponseFrame.encode(header, unsupportedApiVersions()))
      throw ProtocolException(s"unsupported API ${header.apiKey} version ${header.apiVersion}")

    val response = header.apiKey match
      case ApiKey.ApiVersions  => apiVersions(header.apiVersion, body)
      case ApiKey.Metadata     => metadata(body)
      case ApiKey.CreateTopics => createTopics(body)
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

  private def metadata(cursor: ByteCursor): Option[Array[Byte]] =
    val requestedTopics = cursor.readNullableArray(cursor.readString())
    val allowAutoCreation = cursor.readBoolean()
    cursor.ensureFullyRead()

    requestedTopics.foreach { names =>
      if config.autoCreateTopics && allowAutoCreation then
        names.foreach(name => registry.getOrCreate(name))
    }
    val topicNames = requestedTopics.getOrElse(registry.topicNames)
    val writer = ByteWriter()
    writer.writeInt(0) // throttle_time_ms
    writer.writeArray(Vector(config.nodeId)) { nodeId =>
      writer.writeInt(nodeId)
      writer.writeString(config.advertisedHost)
      writer.writeInt(advertisedPort)
      writer.writeNullableString(None)
    }
    writer.writeNullableString(Some("cascade-cluster"))
    writer.writeInt(config.nodeId)
    writer.writeArray(topicNames) { topic =>
      registry.partitions(topic) match
        case None =>
          writer.writeShort(Errors.UnknownTopicOrPartition).writeString(topic).writeBoolean(false)
          writer.writeArray(Vector.empty[Int])(_ => ())
        case Some(partitions) =>
          writer.writeShort(Errors.None).writeString(topic).writeBoolean(topic.startsWith("__"))
          writer.writeArray(partitions.indices) { index =>
            writer.writeShort(Errors.None)
            writer.writeInt(index)
            writer.writeInt(config.nodeId)
            writer.writeArray(Vector(config.nodeId))(writer.writeInt)
            writer.writeArray(Vector(config.nodeId))(writer.writeInt)
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
      if topic.replicationFactor != 1 then
        (topic.name, Errors.InvalidReplicationFactor, Some("Cascade single-node mode requires replication factor 1"))
      else if validateOnly then
        (topic.name, Errors.None, None)
      else
        registry.createTopic(topic.name, topic.partitions) match
          case CreateTopicResult.Created => (topic.name, Errors.None, None)
          case CreateTopicResult.AlreadyExists =>
            (topic.name, Errors.TopicAlreadyExists, Some(s"Topic '${topic.name}' already exists"))
          case CreateTopicResult.InvalidPartitions =>
            (topic.name, Errors.InvalidPartitions, Some("Partition count must be positive"))
          case CreateTopicResult.InvalidName =>
            (topic.name, Errors.InvalidTopic, Some("Invalid topic name"))
    }
    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeArray(results) { case (name, error, message) =>
      writer.writeString(name).writeShort(error).writeNullableString(message): Unit
    }
    Some(writer.result())

  private def produce(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readNullableString() // transactional_id
    val acknowledgements = cursor.readShort()
    cursor.readInt() // timeout_ms
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
      if config.autoCreateTopics then registry.getOrCreate(topic): Unit
      val partitionResults = partitions.map { case (index, records) =>
        (registry.partition(topic, index), records) match
          case (Some(log), Some(batch)) =>
            val result = log.append(batch, force = acknowledgements != 0)
            (index, Errors.None, result.baseOffset)
          case _ => (index, Errors.UnknownTopicOrPartition, -1L)
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
    cursor.readByte() // isolation_level
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
        registry.partition(topic, index) match
          case None => (index, Errors.UnknownTopicOrPartition, None)
          case Some(log) =>
            val budget = math.min(math.max(0, partitionMaxBytes), responseBudget)
            val result = log.fetch(offset, budget)
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
        writer.writeLong(result.map(_.highWatermark).getOrElse(-1L))
        writer.writeLong(result.map(_.logStartOffset).getOrElse(-1L))
        writer.writeInt(-1) // aborted_transactions: null
        writer.writeNullableBytes(result.map(_.records))
      }
    }
    Some(writer.result())

  private def listOffsets(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readInt() // replica_id
    cursor.readByte() // isolation_level
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
        registry.partition(topic, index) match
          case Some(log) =>
            writer.writeInt(index).writeShort(Errors.None).writeLong(-1L).writeLong(log.offsetForTimestamp(timestamp)): Unit
          case None =>
            writer.writeInt(index).writeShort(Errors.UnknownTopicOrPartition).writeLong(-1L).writeLong(-1L): Unit
      }
    }
    Some(writer.result())

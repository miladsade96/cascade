package cascade.broker

import cascade.cluster.*
import cascade.delivery.*
import cascade.group.*
import cascade.protocol.*
import cascade.security.*
import cascade.operations.{AuthenticationMetrics, PeerSecurityMetrics}
import cascade.storage.TopicRegistry
import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Arrays

final class RequestHandler(
    config: BrokerConfig,
    registry: TopicRegistry,
    groupCoordinator: GroupCoordinator,
    clusterManager: ClusterManager,
    replicationManager: ReplicationManager,
    deliveryCoordinator: DeliveryCoordinator,
    advertisedPort: Int,
    peerSecurityMetrics: PeerSecurityMetrics = PeerSecurityMetrics(),
    authenticationMetrics: AuthenticationMetrics = AuthenticationMetrics()
) extends AutoCloseable:
  private val credentials = config.security.authentication.credentialsFile.map { path =>
    ReloadableCredentials(path, config.security.authentication.reloadIntervalMillis)
  }
  private val scramCredentials = config.security.authentication.scramCredentialsFile.map { path =>
    ReloadableScramCredentials(path, config.security.authentication.reloadIntervalMillis)
  }
  private val oauthJwks = Option.when(config.security.authentication.mechanisms.exists(_.oauth)) {
    ReloadableJwks(config.security.authentication.oauth)
  }
  private val oauthValidator = oauthJwks.map(JwtValidator(config.security.authentication.oauth, _))
  private val oauthAuthenticator = oauthValidator.map(OAuthBearerAuthenticator(config.security.authentication.oauth, _))
  private val authorizer = config.security.authorization.aclFile.map { path =>
    ReloadableAuthorizer(path, config.security.authorization.superUsers, config.security.authorization.reloadIntervalMillis)
  }
  private val peerAuthenticator = PeerAuthenticator(config.security.peer)
  private val audit = config.security.audit.path.map(path => AuditLog.open(path, config.security.audit.forceEachEvent))

  def auditTransport(session: ConnectionSession): Unit =
    if session.secure then
      recordAudit("transport_authentication", session, "allowed")

  def peerIdentityReloadError: Option[String] = peerAuthenticator.lastReloadError

  def credentialReloadError: Option[String] =
    credentials.flatMap(_.lastReloadError)
      .orElse(scramCredentials.flatMap(_.lastReloadError))
      .orElse(oauthJwks.flatMap(_.lastReloadError))

  override def close(): Unit =
    oauthJwks.foreach(_.close())
    audit.foreach(_.close())

  def handle(frame: Array[Byte]): Option[Array[Byte]] = handle(frame, ConnectionSession.LocalAnonymous)

  def handle(frame: Array[Byte], session: ConnectionSession): Option[Array[Byte]] =
    val (header, body) = RequestHeader.decode(frame)
    if InternalApi.contains(header.apiKey) then
      peerAuthenticator.authenticate(header.clientId, session) match
        case Left(reason) =>
          peerSecurityMetrics.recordRejected()
          recordAudit(
            "peer_authentication",
            session,
            "denied",
            Some(AclOperation.ClusterAction.toString),
            Some(ResourceType.Cluster.toString),
            header.clientId
          )
          throw ProtocolException(s"peer authentication failed: $reason")
        case Right(peer) =>
          peerSecurityMetrics.recordAuthenticated(peer.encrypted)
          recordAudit(
            "peer_authentication",
            session,
            "allowed",
            Some(AclOperation.ClusterAction.toString),
            Some(ResourceType.Cluster.toString),
            Some(peer.nodeId.map(id => s"node-$id").getOrElse("legacy-peer"))
          )
      requireAuthorized(session, AclOperation.ClusterAction, ResourceType.Cluster, "cascade")
      val response = header.apiKey match
        case InternalApi.ReplicaAppend | InternalApi.ReplicaCommit | InternalApi.ReplicaCatchUp |
            InternalApi.ReplicaReset | InternalApi.ReplicaRecoveryComplete | InternalApi.ReplicaRecoveryState |
            InternalApi.ReplicaRecoveryProbe | InternalApi.ReplicaTruncate =>
          replicationManager.handleInternal(header.apiKey, body)
        case _ => clusterManager.handleInternal(header.apiKey, body)
      return Some(ResponseFrame.encode(header, response))
    if !Compatibility.accepts(header.apiKey, header.apiVersion) then
      if header.apiKey == ApiKey.ApiVersions then
        return Some(ResponseFrame.encode(header, unsupportedApiVersions()))
      throw ProtocolException(s"unsupported API ${header.apiKey} version ${header.apiVersion}")

    if config.security.protocol.sasl && !session.authenticated &&
        header.apiKey != ApiKey.ApiVersions && header.apiKey != ApiKey.SaslHandshake && header.apiKey != ApiKey.SaslAuthenticate
    then throw ProtocolException("Kafka request received before SASL authentication")
    authorizeControlRequest(header.apiKey, frame, session)

    val response = header.apiKey match
      case ApiKey.ApiVersions  => apiVersions(header.apiVersion, body)
      case ApiKey.SaslHandshake => saslHandshake(body, session)
      case ApiKey.SaslAuthenticate => saslAuthenticate(body, session)
      case ApiKey.Metadata     => metadata(body, session)
      case ApiKey.OffsetCommit => offsetCommit(body)
      case ApiKey.OffsetFetch  => offsetFetch(body)
      case ApiKey.FindCoordinator => findCoordinator(body)
      case ApiKey.JoinGroup    => joinGroup(header, body)
      case ApiKey.Heartbeat    => heartbeat(body)
      case ApiKey.LeaveGroup   => leaveGroup(body)
      case ApiKey.SyncGroup    => syncGroup(body)
      case ApiKey.CreateTopics => createTopics(body, session)
      case ApiKey.DescribeAcls => describeAcls(body, session)
      case ApiKey.CreateAcls => createAcls(body, session)
      case ApiKey.DeleteAcls => deleteAcls(body, session)
      case ApiKey.DescribeConfigs => describeConfigs(body)
      case ApiKey.AlterPartitionReassignments => alterPartitionReassignments(body)
      case ApiKey.ListPartitionReassignments => listPartitionReassignments(body)
      case ApiKey.DescribeQuorum => describeQuorum(header.apiVersion, body)
      case ApiKey.AddRaftVoter => addRaftVoter(header.apiVersion, body)
      case ApiKey.RemoveRaftVoter => removeRaftVoter(body)
      case ApiKey.InitProducerId => initProducerId(body)
      case ApiKey.AddPartitionsToTxn => addPartitionsToTxn(body)
      case ApiKey.AddOffsetsToTxn => addOffsetsToTxn(body)
      case ApiKey.EndTxn => endTxn(body)
      case ApiKey.TxnOffsetCommit => txnOffsetCommit(body)
      case ApiKey.Produce      => produce(body, session)
      case ApiKey.Fetch        => fetch(body, session)
      case ApiKey.ListOffsets  => listOffsets(body, session)
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

  private def saslHandshake(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
    val mechanism = cursor.readString()
    cursor.ensureFullyRead()
    val enabled = config.security.authentication.mechanisms
    val error =
      if !config.security.protocol.sasl || session.authenticated then Errors.IllegalSaslState
      else
        enabled.find(_.wireName == mechanism) match
          case None => Errors.UnsupportedSaslMechanism
          case Some(SaslMechanism.Plain) =>
            session.selectMechanism(mechanism)
            Errors.None
          case Some(selected) if selected.scram =>
            val exchange = ScramServerSession(
              selected,
              user => scramCredentials.flatMap(_.credential(selected, user))
            )
            session.selectScramMechanism(selected, exchange)
            Errors.None
          case Some(SaslMechanism.OAuthBearer) =>
            session.selectMechanism(mechanism)
            Errors.None
          case Some(_) => Errors.UnsupportedSaslMechanism
    if error != Errors.None then session.terminateAfterResponse()
    val writer = ByteWriter().writeShort(error)
    writer.writeArray(enabled.map(_.wireName))(writer.writeString)
    Some(writer.result())

  private def saslAuthenticate(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
    val token = cursor.readByteArray()
    cursor.ensureFullyRead()
    try
      if !config.security.protocol.sasl || session.authenticated then
        session.terminateAfterResponse()
        authenticationResponse(Errors.IllegalSaslState, Some("SASL authentication is not in progress"), Array.emptyByteArray)
      else
        session.mechanism.flatMap(value => config.security.authentication.mechanisms.find(_.wireName == value)) match
          case Some(SaslMechanism.Plain) => authenticatePlain(token, session)
          case Some(mechanism) if mechanism.scram => authenticateScram(token, session)
          case Some(SaslMechanism.OAuthBearer) => authenticateOAuth(token, session)
          case _ => authenticationFailure(session, Array.emptyByteArray)
    finally Arrays.fill(token, 0.toByte)

  private def authenticatePlain(token: Array[Byte], session: ConnectionSession): Option[Array[Byte]] =
    val authenticated = parsePlainToken(token).filter { case (user, password) =>
      try credentials.exists(_.authenticate(user, password))
      finally Arrays.fill(password, '\u0000')
    }.map(_._1)
    authenticated match
      case Some(principal) => authenticationSuccess(session, principal, Array.emptyByteArray)
      case None            => authenticationFailure(session, Array.emptyByteArray)

  private def authenticateScram(token: Array[Byte], session: ConnectionSession): Option[Array[Byte]] =
    session.evaluateScram(token) match
      case Some(ScramChallenge(bytes)) => authenticationResponse(Errors.None, None, bytes)
      case Some(ScramSuccess(principal, bytes)) => authenticationSuccess(session, principal, bytes)
      case Some(ScramFailure(_, bytes)) => authenticationFailure(session, bytes)
      case None => authenticationFailure(session, Array.emptyByteArray)

  private def authenticateOAuth(token: Array[Byte], session: ConnectionSession): Option[Array[Byte]] =
    val authenticated = oauthAuthenticator.flatMap(_.authenticate(token))
    authenticated match
      case Some(identity) =>
        authenticationSuccess(
          session,
          identity.principal,
          Array.emptyByteArray,
          expiresAtEpochMillis = identity.expiresAtEpochMillis,
          reportedSessionLifetimeMillis = 0L,
          roles = identity.roles
        )
      case None => authenticationFailure(session, Array.emptyByteArray)

  private def authenticationSuccess(
      session: ConnectionSession,
      principal: String,
      authenticationBytes: Array[Byte],
      expiresAtEpochMillis: Long = Long.MaxValue,
      reportedSessionLifetimeMillis: Long = config.security.authentication.sessionLifetimeMillis,
      roles: Set[String] = Set.empty
  ): Option[Array[Byte]] =
    session.authenticate(principal, expiresAtEpochMillis, roles)
    authenticationMetrics.recordSuccess(session.mechanism)
    recordAudit("authentication", session, "allowed", mechanism = session.mechanism)
    authenticationResponse(Errors.None, None, authenticationBytes, reportedSessionLifetimeMillis)

  private def authenticationFailure(
      session: ConnectionSession,
      authenticationBytes: Array[Byte]
  ): Option[Array[Byte]] =
    val mechanism = session.mechanism
    authenticationMetrics.recordFailure(mechanism)
    session.rejectAuthentication()
    recordAudit("authentication", session, "denied", mechanism = mechanism)
    authenticationResponse(Errors.SaslAuthenticationFailed, Some("authentication failed"), authenticationBytes)

  private def authenticationResponse(
      error: Short,
      message: Option[String],
      authenticationBytes: Array[Byte],
      sessionLifetimeMillis: Long = config.security.authentication.sessionLifetimeMillis
  ): Option[Array[Byte]] =
    Some(
      ByteWriter()
        .writeShort(error)
        .writeNullableString(message)
        .writeByteArray(authenticationBytes)
        .writeLong(sessionLifetimeMillis)
        .result()
    )

  private def parsePlainToken(token: Array[Byte]): Option[(String, Array[Char])] =
    val first = token.indexOf(0.toByte)
    val second = if first < 0 then -1 else token.indexOf(0.toByte, first + 1)
    val third = if second < 0 then -1 else token.indexOf(0.toByte, second + 1)
    if first < 0 || second <= first + 0 || third >= 0 then None
    else
      try
        val authorizationId = decodeUtf8(token, 0, first)
        val authenticationId = decodeUtf8(token, first + 1, second - first - 1)
        val password = decodeUtf8(token, second + 1, token.length - second - 1).toCharArray
        if authenticationId.isEmpty || (authorizationId.nonEmpty && authorizationId != authenticationId) then
          Arrays.fill(password, '\u0000')
          None
        else Some(authenticationId -> password)
      catch case _: java.nio.charset.CharacterCodingException => None

  private def decodeUtf8(bytes: Array[Byte], offset: Int, length: Int): String =
    StandardCharsets.UTF_8.newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
      .decode(ByteBuffer.wrap(bytes, offset, length))
      .toString

  private def findCoordinator(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readString()
    val coordinatorType = cursor.readByte()
    cursor.ensureFullyRead()
    val supported = coordinatorType == 0.toByte || coordinatorType == 1.toByte
    val coordinator =
      if clusterManager.isEnabled then clusterManager.controllerNode
      else Some(ClusterNode(config.nodeId, config.advertisedHost, advertisedPort))
    val available = supported && coordinator.nonEmpty
    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeShort(if available then Errors.None else Errors.CoordinatorNotAvailable)
    writer.writeNullableString(
      if !supported then Some("unsupported coordinator type")
      else if coordinator.isEmpty then Some("controller election is in progress")
      else None
    )
    writer.writeInt(coordinator.filter(_ => available).map(_.id).getOrElse(-1))
    writer.writeString(coordinator.filter(_ => available).map(_.host).getOrElse(""))
    writer.writeInt(coordinator.filter(_ => available).map(_.port).getOrElse(-1))
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
    if !isCoordinator then
      return Some(
        ByteWriter()
          .writeInt(0)
          .writeShort(Errors.NotCoordinator)
          .writeInt(-1)
          .writeString("")
          .writeString("")
          .writeString(memberId)
          .writeArray(Vector.empty[Unit])(_ => ())
          .result()
      )
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
    val error = if isCoordinator then groupCoordinator.heartbeat(groupId, generationId, memberId) else Errors.NotCoordinator
    Some(ByteWriter().writeInt(0).writeShort(error).result())

  private def leaveGroup(cursor: ByteCursor): Option[Array[Byte]] =
    val groupId = cursor.readString()
    val memberId = cursor.readString()
    cursor.ensureFullyRead()
    val error = if isCoordinator then groupCoordinator.leave(groupId, memberId) else Errors.NotCoordinator
    Some(ByteWriter().writeInt(0).writeShort(error).result())

  private def syncGroup(cursor: ByteCursor): Option[Array[Byte]] =
    val groupId = cursor.readString()
    val generationId = cursor.readInt()
    val memberId = cursor.readString()
    cursor.readNullableString()
    val assignments = cursor.readArray((cursor.readString(), cursor.readByteArray()))
    cursor.ensureFullyRead()
    val result =
      if isCoordinator then groupCoordinator.sync(groupId, generationId, memberId, assignments)
      else SyncGroupResult(Errors.NotCoordinator, Array.emptyByteArray)
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
    val groupError =
      if isCoordinator then groupCoordinator.commitOffsets(groupId, generationId, memberId, validValues)
      else Errors.NotCoordinator
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
            (partition, Option.when(isCoordinator)(groupCoordinator.fetchOffset(key)).flatten)
          }
          (topic, values)
        }
      case None if isCoordinator =>
        groupCoordinator.allOffsets(groupId)
          .groupBy(_._1.topic)
          .toVector
          .sortBy(_._1)
          .map { case (topic, values) =>
            (topic, values.sortBy(_._1.partition).map { case (key, value) => (key.partition, Some(value)) })
          }
      case None => Vector.empty
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(offsets) { case (topic, partitions) =>
      writer.writeString(topic)
      writer.writeArray(partitions) { case (partition, committed) =>
        writer.writeInt(partition)
        writer.writeLong(committed.map(_.offset).getOrElse(-1L))
        writer.writeInt(committed.map(_.leaderEpoch).getOrElse(-1))
        writer.writeNullableString(committed.flatMap(_.metadata))
        writer.writeShort(if isCoordinator then Errors.None else Errors.NotCoordinator): Unit
      }
    }
    writer.writeShort(if isCoordinator then Errors.None else Errors.NotCoordinator)
    Some(writer.result())

  private def metadata(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
    val requestedTopics = cursor.readNullableArray(cursor.readString())
    val allowAutoCreation = cursor.readBoolean()
    cursor.ensureFullyRead()

    requestedTopics.foreach { names =>
      if config.autoCreateTopics && allowAutoCreation then
        names.foreach { name =>
          val canDescribe = isAuthorized(session, AclOperation.Describe, ResourceType.Topic, name)
          val exists = if clusterManager.isEnabled then clusterManager.topic(name).nonEmpty else registry.partitions(name).nonEmpty
          if canDescribe && !exists && isAuthorized(session, AclOperation.Create, ResourceType.Topic, name) then
            if clusterManager.isEnabled then
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
    writer.writeInt(if clusterManager.isEnabled then clusterManager.controllerId else config.nodeId)
    writer.writeArray(topicNames) { topic =>
      val clusterTopic = clusterManager.topic(topic)
      val localPartitions = registry.partitions(topic)
      if !isAuthorized(session, AclOperation.Describe, ResourceType.Topic, topic) then
        writer.writeShort(Errors.TopicAuthorizationFailed).writeString(topic).writeBoolean(false)
        writer.writeArray(Vector.empty[Int])(_ => ())
      else if clusterManager.isEnabled then clusterTopic match
        case None =>
          writer.writeShort(Errors.UnknownTopicOrPartition).writeString(topic).writeBoolean(false)
          writer.writeArray(Vector.empty[Int])(_ => ())
        case Some(metadata) =>
          writer.writeShort(Errors.None).writeString(topic).writeBoolean(topic.startsWith("__"))
          writer.writeArray(metadata.partitions) { partition =>
            val partitionError =
              if clusterManager.isBrokerFenced || partition.leaderId < 0 then Errors.LeaderNotAvailable
              else Errors.None
            writer.writeShort(partitionError)
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

  private def createTopics(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
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
        if !isAuthorized(session, AclOperation.Create, ResourceType.Topic, topic.name) then
          ClusterCreateResult(Errors.TopicAuthorizationFailed, Some("topic authorization failed"))
        else if validateOnly then clusterManager.validateTopic(topic.name, topic.partitions, replicationFactor)
        else clusterManager.createTopic(topic.name, topic.partitions, replicationFactor)
      (topic.name, result.errorCode, result.message)
    }
    val writer = ByteWriter()
    writer.writeInt(0)
    writer.writeArray(results) { case (name, error, message) =>
      writer.writeString(name).writeShort(error).writeNullableString(message): Unit
    }
    Some(writer.result())

  private def describeAcls(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
    val filter = readAclFilter(cursor)
    cursor.ensureFullyRead()
    val (error, message, matching) = authorizer match
      case None => (Errors.SecurityDisabled, Some("authorization is disabled"), Vector.empty[AclRule])
      case Some(_) if !isAuthorized(session, AclOperation.Describe, ResourceType.Cluster, "cascade") =>
        (Errors.ClusterAuthorizationFailed, Some("cluster authorization failed"), Vector.empty[AclRule])
      case Some(current) => filter match
        case Left(reason) => (Errors.InvalidRequest, Some(reason), Vector.empty[AclRule])
        case Right(value) => (Errors.None, None, current.rules.filter(_.matchesFilter(value)))

    val grouped = matching.groupBy(rule => (rule.resourceType, rule.resourcePattern, rule.patternType)).toVector
      .sortBy { case ((resourceType, name, patternType), _) => (resourceType.ordinal, name, patternType.ordinal) }
    val writer = ByteWriter().writeInt(0).writeShort(error).writeNullableString(message)
    writer.writeArray(grouped) { case ((resourceType, name, patternType), rules) =>
      writer.writeByte(resourceTypeCode(resourceType)).writeString(name).writeByte(patternTypeCode(patternType))
      writer.writeArray(rules.sortBy(rule => (rule.principal, rule.host, rule.operation.ordinal, rule.effect.ordinal))) { rule =>
        writeAclEntry(writer, rule): Unit
      }: Unit
    }
    Some(writer.result())

  private def createAcls(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
    val requested = cursor.readArray(readAclRule(cursor))
    cursor.ensureFullyRead()
    val authorized = authorizer.nonEmpty && isAuthorized(session, AclOperation.Alter, ResourceType.Cluster, "cascade")
    val valid = requested.collect { case Right(rule) => rule }
    val persistenceError =
      if authorizer.isEmpty || !authorized || valid.isEmpty then None
      else authorizer.flatMap(_.createRules(valid).left.toOption)
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(requested) { candidate =>
      val (error, message) =
        if authorizer.isEmpty then Errors.SecurityDisabled -> Some("authorization is disabled")
        else if !authorized then Errors.ClusterAuthorizationFailed -> Some("cluster authorization failed")
        else candidate match
          case Left(reason) => Errors.InvalidRequest -> Some(reason)
          case Right(_) => persistenceError match
            case Some(reason) => Errors.KafkaStorageError -> Some(reason)
            case None         => Errors.None -> None
      writer.writeShort(error).writeNullableString(message): Unit
    }
    Some(writer.result())

  private def deleteAcls(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
    val filters = cursor.readArray(readAclFilter(cursor))
    cursor.ensureFullyRead()
    val authorized = authorizer.nonEmpty && isAuthorized(session, AclOperation.Alter, ResourceType.Cluster, "cascade")
    val results = filters.map { filter =>
      if authorizer.isEmpty then (Errors.SecurityDisabled, Some("authorization is disabled"), Vector.empty[AclRule])
      else if !authorized then
        (Errors.ClusterAuthorizationFailed, Some("cluster authorization failed"), Vector.empty[AclRule])
      else filter match
        case Left(reason) => (Errors.InvalidRequest, Some(reason), Vector.empty[AclRule])
        case Right(value) => authorizer.get.deleteRules(value) match
          case Left(reason)  => (Errors.KafkaStorageError, Some(reason), Vector.empty[AclRule])
          case Right(rules)  => (Errors.None, None, rules)
    }
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(results) { case (error, message, rules) =>
      writer.writeShort(error).writeNullableString(message)
      writer.writeArray(rules) { rule =>
        writer.writeShort(Errors.None).writeNullableString(None)
        writer.writeByte(resourceTypeCode(rule.resourceType)).writeString(rule.resourcePattern)
          .writeByte(patternTypeCode(rule.patternType))
        writeAclEntry(writer, rule): Unit
      }: Unit
    }
    Some(writer.result())

  private def readAclRule(cursor: ByteCursor): Either[String, AclRule] =
    val resourceType = cursor.readByte()
    val resourceName = cursor.readString()
    val patternType = cursor.readByte()
    val principal = cursor.readString()
    val host = cursor.readString()
    val operation = cursor.readByte()
    val permission = cursor.readByte()
    for
      decodedResource <- decodeResourceType(resourceType, allowAny = false)
      decodedPattern <- decodePatternType(patternType)
      decodedOperation <- decodeOperation(operation, allowAny = false)
      decodedEffect <- decodeEffect(permission, allowAny = false)
      rule <- scala.util.Try(
        AclRule(decodedEffect.get, principal, decodedOperation.get, decodedResource.get, resourceName, decodedPattern, host)
      ).toEither.left.map(error => Option(error.getMessage).getOrElse("invalid ACL"))
    yield rule

  private def readAclFilter(cursor: ByteCursor): Either[String, AclFilter] =
    val resourceType = cursor.readByte()
    val resourceName = cursor.readNullableString()
    val patternType = cursor.readByte()
    val principal = cursor.readNullableString()
    val host = cursor.readNullableString()
    val operation = cursor.readByte()
    val permission = cursor.readByte()
    for
      decodedResource <- decodeResourceType(resourceType, allowAny = true)
      decodedPattern <- decodePatternFilter(patternType)
      decodedOperation <- decodeOperation(operation, allowAny = true)
      decodedEffect <- decodeEffect(permission, allowAny = true)
    yield AclFilter(decodedResource, resourceName, decodedPattern, principal, host, decodedOperation, decodedEffect)

  private def decodeResourceType(value: Byte, allowAny: Boolean): Either[String, Option[ResourceType]] = value.toInt match
    case 1 if allowAny => Right(None)
    case 2             => Right(Some(ResourceType.Topic))
    case 3             => Right(Some(ResourceType.Group))
    case 4             => Right(Some(ResourceType.Cluster))
    case 5             => Right(Some(ResourceType.TransactionalId))
    case other         => Left(s"unsupported ACL resource type: $other")

  private def decodePatternType(value: Byte): Either[String, AclPatternType] = value.toInt match
    case 3     => Right(AclPatternType.Literal)
    case 4     => Right(AclPatternType.Prefixed)
    case other => Left(s"unsupported ACL pattern type: $other")

  private def decodePatternFilter(value: Byte): Either[String, AclPatternFilter] = value.toInt match
    case 1     => Right(AclPatternFilter.Any)
    case 2     => Right(AclPatternFilter.Match)
    case 3     => Right(AclPatternFilter.Literal)
    case 4     => Right(AclPatternFilter.Prefixed)
    case other => Left(s"unsupported ACL pattern filter: $other")

  private def decodeOperation(value: Byte, allowAny: Boolean): Either[String, Option[AclOperation]] = value.toInt match
    case 1 if allowAny => Right(None)
    case 2             => Right(Some(AclOperation.All))
    case 3             => Right(Some(AclOperation.Read))
    case 4             => Right(Some(AclOperation.Write))
    case 5             => Right(Some(AclOperation.Create))
    case 6             => Right(Some(AclOperation.Delete))
    case 7             => Right(Some(AclOperation.Alter))
    case 8             => Right(Some(AclOperation.Describe))
    case 9             => Right(Some(AclOperation.ClusterAction))
    case 12            => Right(Some(AclOperation.IdempotentWrite))
    case other         => Left(s"unsupported ACL operation: $other")

  private def decodeEffect(value: Byte, allowAny: Boolean): Either[String, Option[AclEffect]] = value.toInt match
    case 1 if allowAny => Right(None)
    case 2             => Right(Some(AclEffect.Deny))
    case 3             => Right(Some(AclEffect.Allow))
    case other         => Left(s"unsupported ACL permission: $other")

  private def resourceTypeCode(value: ResourceType): Int = value match
    case ResourceType.Topic           => 2
    case ResourceType.Group           => 3
    case ResourceType.Cluster         => 4
    case ResourceType.TransactionalId => 5

  private def patternTypeCode(value: AclPatternType): Int = value match
    case AclPatternType.Literal  => 3
    case AclPatternType.Prefixed => 4

  private def operationCode(value: AclOperation): Int = value match
    case AclOperation.All             => 2
    case AclOperation.Read            => 3
    case AclOperation.Write           => 4
    case AclOperation.Create          => 5
    case AclOperation.Delete          => 6
    case AclOperation.Alter           => 7
    case AclOperation.Describe        => 8
    case AclOperation.ClusterAction   => 9
    case AclOperation.IdempotentWrite => 12

  private def effectCode(value: AclEffect): Int = value match
    case AclEffect.Deny  => 2
    case AclEffect.Allow => 3

  private def writeAclEntry(writer: ByteWriter, rule: AclRule): Unit =
    writer.writeString(rule.principal).writeString(rule.host).writeByte(operationCode(rule.operation))
      .writeByte(effectCode(rule.effect)): Unit

  private def describeConfigs(cursor: ByteCursor): Option[Array[Byte]] =
    final case class RequestedResource(resourceType: Byte, name: String, keys: Option[Vector[String]])
    final case class VisibleConfig(name: String, value: String, source: Int)
    final case class ConfigResult(
        resourceType: Byte,
        name: String,
        errorCode: Short,
        errorMessage: Option[String],
        values: Vector[VisibleConfig]
    )
    val requests = cursor.readArray {
      RequestedResource(cursor.readByte(), cursor.readString(), cursor.readNullableArray(cursor.readString()))
    }
    cursor.readBoolean() // include_synonyms
    cursor.ensureFullyRead()

    val brokerValues = Vector(
      VisibleConfig("broker.id", config.nodeId.toString, 4),
      VisibleConfig("log.dirs", config.dataDirectory.toAbsolutePath.normalize().toString, 4),
      VisibleConfig("message.max.bytes", config.maxRequestBytes.toString, 4),
      VisibleConfig("log.segment.bytes", config.segmentBytes.toString, 4),
      VisibleConfig("log.retention.ms", config.storageLifecycle.retentionMillis.toString, 4),
      VisibleConfig("log.retention.bytes", config.storageLifecycle.retentionBytes.toString, 4),
      VisibleConfig("log.cleanup.policy", cleanupPolicyName, 4),
      VisibleConfig("num.partitions", "1", 4),
      VisibleConfig("default.replication.factor", config.defaultReplicationFactor.toString, 4),
      VisibleConfig("min.insync.replicas", config.minInSyncReplicas.toString, 4),
      VisibleConfig("auto.create.topics.enable", config.autoCreateTopics.toString, 4)
    )
    val topicValues = Vector(
      VisibleConfig("cleanup.policy", cleanupPolicyName, 5),
      VisibleConfig("retention.ms", config.storageLifecycle.retentionMillis.toString, 5),
      VisibleConfig("retention.bytes", config.storageLifecycle.retentionBytes.toString, 5),
      VisibleConfig("segment.bytes", config.segmentBytes.toString, 5),
      VisibleConfig("max.message.bytes", config.maxRequestBytes.toString, 5),
      VisibleConfig("min.insync.replicas", config.minInSyncReplicas.toString, 5)
    )
    def selected(values: Vector[VisibleConfig], keys: Option[Vector[String]]): Vector[VisibleConfig] =
      keys match
        case None => values
        case Some(requested) =>
          val byName = values.iterator.map(value => value.name -> value).toMap
          requested.flatMap(byName.get)

    val results = requests.map { request =>
      request.resourceType.toInt match
        case 4 if request.name.isEmpty || request.name == config.nodeId.toString =>
          ConfigResult(request.resourceType, request.name, Errors.None, None, selected(brokerValues, request.keys))
        case 4 =>
          ConfigResult(request.resourceType, request.name, Errors.BrokerNotAvailable, Some("broker is not available"), Vector.empty)
        case 2 if topicExists(request.name) =>
          ConfigResult(request.resourceType, request.name, Errors.None, None, selected(topicValues, request.keys))
        case 2 =>
          ConfigResult(request.resourceType, request.name, Errors.UnknownTopicOrPartition, Some("topic does not exist"), Vector.empty)
        case _ =>
          ConfigResult(request.resourceType, request.name, Errors.InvalidRequest, Some("unsupported config resource type"), Vector.empty)
    }

    val writer = ByteWriter().writeInt(0)
    writer.writeArray(results) { result =>
      writer.writeShort(result.errorCode)
      writer.writeNullableString(result.errorMessage)
      writer.writeByte(result.resourceType)
      writer.writeString(result.name)
      writer.writeArray(result.values) { value =>
        writer.writeString(value.name)
        writer.writeNullableString(Some(value.value))
        writer.writeBoolean(true) // Cascade does not expose incremental config mutation yet.
        writer.writeByte(value.source)
        writer.writeBoolean(false)
        writer.writeArray(Vector.empty[Unit])(_ => ()): Unit
      }
    }
    Some(writer.result())

  private def alterPartitionReassignments(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readInt() // timeout_ms; the metadata quorum bounds the operation.
    val requests = cursor.readCompactArray {
      val topic = cursor.readCompactString()
      val partitions = cursor.readCompactArray {
        val partition = cursor.readInt()
        val replicas = cursor.readCompactNullableArray(cursor.readInt())
        cursor.skipTaggedFields()
        PartitionReassignmentRequest(topic, partition, replicas)
      }
      cursor.skipTaggedFields()
      partitions
    }.flatten
    cursor.skipTaggedFields()
    cursor.ensureFullyRead()

    val result = clusterManager.alterPartitionReassignments(requests)
    val writer = ByteWriter()
      .writeInt(0)
      .writeShort(result.errorCode)
      .writeCompactNullableString(result.message)
    writer.writeCompactArray(result.partitions.groupBy(_.topic).toVector.sortBy(_._1)) { case (topic, partitions) =>
      writer.writeCompactString(topic)
      writer.writeCompactArray(partitions.sortBy(_.partition)) { partition =>
        writer
          .writeInt(partition.partition)
          .writeShort(partition.errorCode)
          .writeCompactNullableString(partition.message)
          .writeEmptyTaggedFields(): Unit
      }
      writer.writeEmptyTaggedFields(): Unit
    }
    writer.writeEmptyTaggedFields()
    Some(writer.result())

  private def listPartitionReassignments(cursor: ByteCursor): Option[Array[Byte]] =
    cursor.readInt() // timeout_ms
    val requested = cursor.readCompactNullableArray {
      val topic = cursor.readCompactString()
      val partitions = cursor.readCompactArray(cursor.readInt())
      cursor.skipTaggedFields()
      partitions.map(topic -> _)
    }.map(_.flatten.toSet)
    cursor.skipTaggedFields()
    cursor.ensureFullyRead()

    val result = clusterManager.listPartitionReassignments(requested)
    val writer = ByteWriter()
      .writeInt(0)
      .writeShort(result.errorCode)
      .writeCompactNullableString(result.message)
    writer.writeCompactArray(result.partitions.groupBy(_.topic).toVector.sortBy(_._1)) { case (topic, partitions) =>
      writer.writeCompactString(topic)
      writer.writeCompactArray(partitions.sortBy(_.partition)) { partition =>
        writer.writeInt(partition.partition)
        writer.writeCompactArray(partition.replicas)(writer.writeInt)
        writer.writeCompactArray(partition.addingReplicas)(writer.writeInt)
        writer.writeCompactArray(partition.removingReplicas)(writer.writeInt)
        writer.writeEmptyTaggedFields(): Unit
      }
      writer.writeEmptyTaggedFields(): Unit
    }
    writer.writeEmptyTaggedFields()
    Some(writer.result())

  private def addRaftVoter(version: Short, cursor: ByteCursor): Option[Array[Byte]] =
    final case class Listener(name: String, host: String, port: Int)
    val clusterId = cursor.readCompactNullableString()
    val timeoutMillis = cursor.readInt()
    val voterId = cursor.readInt()
    val (directoryHigh, directoryLow) = cursor.readUuid()
    val listeners = cursor.readCompactArray {
      val listener = Listener(cursor.readCompactString(), cursor.readCompactString(), cursor.readUnsignedShort())
      cursor.skipTaggedFields()
      listener
    }
    if version >= 1 then cursor.readBoolean(): Unit // Cascade always waits for the stable configuration.
    cursor.skipTaggedFields()
    cursor.ensureFullyRead()

    val directoryId = VoterDirectoryId(directoryHigh, directoryLow)
    val result =
      if clusterId.exists(_ != "cascade-cluster") then
        MembershipChangeResult(Errors.InconsistentClusterId, Some("cluster ID does not match cascade-cluster"))
      else if timeoutMillis <= 0 || voterId < 0 || directoryId.isZero then
        MembershipChangeResult(Errors.InvalidVoterKey, Some("voter ID, directory ID, or timeout is invalid"))
      else if listeners.isEmpty || listeners.map(_.name).distinct.size != listeners.size then
        MembershipChangeResult(Errors.InvalidRequest, Some("at least one uniquely named listener is required"))
      else
        val listener = listeners.find(_.name == "CONTROLLER").getOrElse(listeners.head)
        if listener.host.isEmpty || listener.port <= 0 then
          MembershipChangeResult(Errors.InvalidRequest, Some("voter listener endpoint is invalid"))
        else
          clusterManager.addVoter(
            QuorumVoter(ClusterNode(voterId, listener.host, listener.port), directoryId)
          )
    Some(membershipChangeResponse(result))

  private def describeQuorum(version: Short, cursor: ByteCursor): Option[Array[Byte]] =
    val requested = cursor.readCompactArray {
      val topic = cursor.readCompactString()
      val partitions = cursor.readCompactArray {
        val partition = cursor.readInt()
        cursor.skipTaggedFields()
        partition
      }
      cursor.skipTaggedFields()
      topic -> partitions
    }
    cursor.skipTaggedFields()
    cursor.ensureFullyRead()

    val membership = clusterManager.quorumMembership
    val voters = membership.voters
    val writer = ByteWriter().writeShort(Errors.None)
    if version >= 2 then writer.writeCompactNullableString(None)
    writer.writeCompactArray(requested) { case (topic, partitions) =>
      writer.writeCompactString(topic)
      writer.writeCompactArray(partitions) { partition =>
        val valid = topic == "__cluster_metadata" && partition == 0
        writer.writeInt(partition)
        writer.writeShort(if valid then Errors.None else Errors.UnknownTopicOrPartition)
        if version >= 2 then
          writer.writeCompactNullableString(Option.when(!valid)("Cascade only exposes __cluster_metadata-0"))
        writer.writeInt(if valid then clusterManager.controllerId else -1)
        writer.writeInt(if valid then math.min(Int.MaxValue.toLong, clusterManager.controllerTerm).toInt else 0)
        writer.writeLong(if valid then clusterManager.metadataVersion else -1L)
        writeReplicaStates(writer, voters, version, valid)
        writer.writeCompactArray(Vector.empty[QuorumVoter])(_ => ())
        writer.writeEmptyTaggedFields(): Unit
      }
      writer.writeEmptyTaggedFields(): Unit
    }
    if version >= 2 then
      writer.writeCompactArray(voters) { voter =>
        writer.writeInt(voter.id)
        writer.writeCompactArray(Vector(voter.node)) { node =>
          writer.writeCompactString("CONTROLLER")
          writer.writeCompactString(node.host)
          writer.writeShort(node.port)
          writer.writeEmptyTaggedFields(): Unit
        }
        writer.writeEmptyTaggedFields(): Unit
      }
    writer.writeEmptyTaggedFields()
    Some(writer.result())

  private def writeReplicaStates(
      writer: ByteWriter,
      voters: Vector[QuorumVoter],
      version: Short,
      valid: Boolean
  ): Unit =
    writer.writeCompactArray(voters) { voter =>
      writer.writeInt(voter.id)
      if version >= 2 then
        writer.writeUuid(voter.directoryId.mostSignificantBits, voter.directoryId.leastSignificantBits)
      writer.writeLong(if valid && voter.id == clusterManager.controllerId then clusterManager.metadataVersion else -1L)
      if version >= 1 then
        writer.writeLong(-1L)
        writer.writeLong(if valid && voter.id == clusterManager.controllerId then System.currentTimeMillis() else -1L)
      writer.writeEmptyTaggedFields(): Unit
    }: Unit

  private def removeRaftVoter(cursor: ByteCursor): Option[Array[Byte]] =
    val clusterId = cursor.readCompactNullableString()
    val voterId = cursor.readInt()
    val (directoryHigh, directoryLow) = cursor.readUuid()
    cursor.skipTaggedFields()
    cursor.ensureFullyRead()

    val result =
      if clusterId.exists(_ != "cascade-cluster") then
        MembershipChangeResult(Errors.InconsistentClusterId, Some("cluster ID does not match cascade-cluster"))
      else if voterId < 0 then MembershipChangeResult(Errors.InvalidVoterKey, Some("voter ID is invalid"))
      else clusterManager.removeVoter(voterId, VoterDirectoryId(directoryHigh, directoryLow))
    Some(membershipChangeResponse(result))

  private def membershipChangeResponse(result: MembershipChangeResult): Array[Byte] =
    ByteWriter()
      .writeInt(0)
      .writeShort(result.errorCode)
      .writeCompactNullableString(result.message)
      .writeEmptyTaggedFields()
      .result()

  private def produce(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
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
      val topicAuthorized = isAuthorized(session, AclOperation.Write, ResourceType.Topic, topic)
      val transactionAuthorized = transactionalId.forall { id =>
        isAuthorized(session, AclOperation.Write, ResourceType.TransactionalId, id)
      }
      if topicAuthorized && transactionAuthorized && config.autoCreateTopics then
        if clusterManager.isEnabled then
          if clusterManager.topic(topic).isEmpty && isAuthorized(session, AclOperation.Create, ResourceType.Topic, topic) then
            clusterManager.createTopic(topic, 1, config.defaultReplicationFactor): Unit
        else if registry.partitions(topic).nonEmpty || isAuthorized(session, AclOperation.Create, ResourceType.Topic, topic) then
          registry.getOrCreate(topic): Unit
      val partitionResults = partitions.map { case (index, records) =>
        if !topicAuthorized then (index, Errors.TopicAuthorizationFailed, -1L)
        else if !transactionAuthorized then (index, Errors.TransactionalIdAuthorizationFailed, -1L)
        else records match
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

  private def fetch(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
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
        if !isAuthorized(session, AclOperation.Read, ResourceType.Topic, topic) then
          (index, Errors.TopicAuthorizationFailed, None)
        else if clusterManager.isEnabled && clusterManager.isBrokerFenced then
          (index, Errors.BrokerNotAvailable, None)
        else if clusterManager.isEnabled && partitionMetadata.isEmpty then
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

  private def listOffsets(cursor: ByteCursor, session: ConnectionSession): Option[Array[Byte]] =
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
        if !isAuthorized(session, AclOperation.Read, ResourceType.Topic, topic) then
          writer.writeInt(index).writeShort(Errors.TopicAuthorizationFailed).writeLong(-1L).writeLong(-1L): Unit
        else if clusterManager.isEnabled && clusterManager.isBrokerFenced then
          writer.writeInt(index).writeShort(Errors.BrokerNotAvailable).writeLong(-1L).writeLong(-1L): Unit
        else if clusterManager.isEnabled && partitionMetadata.isEmpty then
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
    val result =
      if isCoordinator then deliveryCoordinator.initProducerId(transactionalId, timeoutMillis)
      else InitProducerIdResult(Errors.NotCoordinator, -1L, -1)
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
    val transactionError =
      if isCoordinator then deliveryCoordinator.addPartitions(transactionalId, producerId, producerEpoch, valid)
      else Errors.NotCoordinator
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
    val error =
      if isCoordinator then deliveryCoordinator.addOffsets(transactionalId, producerId, producerEpoch, groupId)
      else Errors.NotCoordinator
    Some(ByteWriter().writeInt(0).writeShort(error).result())

  private def endTxn(cursor: ByteCursor): Option[Array[Byte]] =
    val transactionalId = cursor.readString()
    val producerId = cursor.readLong()
    val producerEpoch = cursor.readShort()
    val committed = cursor.readBoolean()
    cursor.ensureFullyRead()
    val error =
      if isCoordinator then deliveryCoordinator.endTransaction(transactionalId, producerId, producerEpoch, committed)
      else Errors.NotCoordinator
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
      if isCoordinator then deliveryCoordinator.stageOffsets(transactionalId, producerId, producerEpoch, groupId, values)
      else Errors.NotCoordinator
    val writer = ByteWriter().writeInt(0)
    writer.writeArray(requested) { case (topic, offsets) =>
      writer.writeString(topic)
      writer.writeArray(offsets) { offset =>
        val error = if offset.exists then transactionError else Errors.UnknownTopicOrPartition
        writer.writeInt(offset.index).writeShort(error): Unit
      }
    }
    Some(writer.result())

  private def authorizeControlRequest(apiKey: Short, frame: Array[Byte], session: ConnectionSession): Unit =
    if authorizer.isEmpty then return
    val (_, cursor) = RequestHeader.decode(frame)
    apiKey match
      case ApiKey.OffsetCommit | ApiKey.OffsetFetch | ApiKey.JoinGroup | ApiKey.Heartbeat | ApiKey.LeaveGroup |
          ApiKey.SyncGroup =>
        requireAuthorized(session, AclOperation.Read, ResourceType.Group, cursor.readString())
      case ApiKey.FindCoordinator =>
        val resource = cursor.readString()
        val coordinatorType = cursor.readByte()
        if coordinatorType == 0.toByte then requireAuthorized(session, AclOperation.Describe, ResourceType.Group, resource)
        else requireAuthorized(session, AclOperation.Describe, ResourceType.TransactionalId, resource)
      case ApiKey.InitProducerId =>
        cursor.readNullableString() match
          case Some(transactionalId) =>
            requireAuthorized(session, AclOperation.Write, ResourceType.TransactionalId, transactionalId)
          case None => requireAuthorized(session, AclOperation.IdempotentWrite, ResourceType.Cluster, "cascade")
      case ApiKey.AddPartitionsToTxn =>
        val transactionalId = cursor.readString()
        requireAuthorized(session, AclOperation.Write, ResourceType.TransactionalId, transactionalId)
        cursor.readLong()
        cursor.readShort()
        cursor.readArray {
          val topic = cursor.readString()
          requireAuthorized(session, AclOperation.Write, ResourceType.Topic, topic)
          cursor.readArray(cursor.readInt())
        }: Unit
      case ApiKey.AddOffsetsToTxn =>
        requireAuthorized(session, AclOperation.Write, ResourceType.TransactionalId, cursor.readString())
        cursor.readLong()
        cursor.readShort()
        requireAuthorized(session, AclOperation.Read, ResourceType.Group, cursor.readString())
      case ApiKey.EndTxn =>
        requireAuthorized(session, AclOperation.Write, ResourceType.TransactionalId, cursor.readString())
      case ApiKey.TxnOffsetCommit =>
        requireAuthorized(session, AclOperation.Write, ResourceType.TransactionalId, cursor.readString())
        requireAuthorized(session, AclOperation.Read, ResourceType.Group, cursor.readString())
      case ApiKey.AlterPartitionReassignments | ApiKey.AddRaftVoter | ApiKey.RemoveRaftVoter =>
        requireAuthorized(session, AclOperation.Alter, ResourceType.Cluster, "cascade")
      case ApiKey.ListPartitionReassignments | ApiKey.DescribeQuorum =>
        requireAuthorized(session, AclOperation.Describe, ResourceType.Cluster, "cascade")
      case ApiKey.DescribeConfigs =>
        cursor.readArray {
          val resourceType = cursor.readByte()
          val resourceName = cursor.readString()
          cursor.readNullableArray(cursor.readString())
          if resourceType == 2.toByte then
            requireAuthorized(session, AclOperation.Describe, ResourceType.Topic, resourceName)
          else if resourceType == 4.toByte then
            requireAuthorized(session, AclOperation.Describe, ResourceType.Cluster, "cascade")
        }: Unit
      case _ => ()

  private def isAuthorized(
      session: ConnectionSession,
      operation: AclOperation,
      resourceType: ResourceType,
      resourceName: String
  ): Boolean =
    authorizer match
      case None => true
      case Some(current) =>
        val allowed = current.authorizeAny(session.authorizationPrincipals, operation, Resource(resourceType, resourceName), session.remoteAddress)
        recordAudit(
          "authorization",
          session,
          if allowed then "allowed" else "denied",
          Some(operation.toString),
          Some(resourceType.toString),
          Some(resourceName)
        )
        allowed

  private def requireAuthorized(
      session: ConnectionSession,
      operation: AclOperation,
      resourceType: ResourceType,
      resourceName: String
  ): Unit =
    if !isAuthorized(session, operation, resourceType, resourceName) then
      throw ProtocolException(s"${operation.toString.toLowerCase} authorization failed for ${resourceType.toString.toLowerCase}")

  private def recordAudit(
      eventType: String,
      session: ConnectionSession,
      decision: String,
      operation: Option[String] = None,
      resourceType: Option[String] = None,
      resource: Option[String] = None,
      mechanism: Option[String] = None
  ): Unit =
    audit.foreach(
      _.record(
        AuditEvent(
          eventType,
          session.principal,
          session.remoteAddress,
          session.secure,
          decision,
          operation,
          resourceType,
          resource,
          mechanism
        )
      )
    )

  private def partitionExists(topic: String, partition: Int): Boolean =
    if clusterManager.isEnabled then clusterManager.partition(topic, partition).nonEmpty
    else registry.partition(topic, partition).nonEmpty

  private def topicExists(topic: String): Boolean =
    if clusterManager.isEnabled then clusterManager.topic(topic).nonEmpty
    else registry.partitions(topic).nonEmpty

  private def cleanupPolicyName: String = config.storageLifecycle.cleanupPolicy match
    case cascade.storage.CleanupPolicy.Delete        => "delete"
    case cascade.storage.CleanupPolicy.Compact       => "compact"
    case cascade.storage.CleanupPolicy.CompactDelete => "compact,delete"

  private def isCoordinator: Boolean = !clusterManager.isEnabled || clusterManager.isActiveController

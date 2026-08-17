package cascade.protocol

object ApiKey:
  val Produce: Short = 0
  val Fetch: Short = 1
  val ListOffsets: Short = 2
  val Metadata: Short = 3
  val OffsetCommit: Short = 8
  val OffsetFetch: Short = 9
  val FindCoordinator: Short = 10
  val JoinGroup: Short = 11
  val Heartbeat: Short = 12
  val LeaveGroup: Short = 13
  val SyncGroup: Short = 14
  val ApiVersions: Short = 18
  val CreateTopics: Short = 19
  val InitProducerId: Short = 22
  val AddPartitionsToTxn: Short = 24
  val AddOffsetsToTxn: Short = 25
  val EndTxn: Short = 26
  val TxnOffsetCommit: Short = 28
  val AlterPartitionReassignments: Short = 45
  val ListPartitionReassignments: Short = 46
  val DescribeQuorum: Short = 55
  val AddRaftVoter: Short = 80
  val RemoveRaftVoter: Short = 81

object Errors:
  val None: Short = 0
  val UnknownTopicOrPartition: Short = 3
  val LeaderNotAvailable: Short = 5
  val NotLeaderOrFollower: Short = 6
  val RequestTimedOut: Short = 7
  val BrokerNotAvailable: Short = 8
  val ReplicaNotAvailable: Short = 9
  val NotEnoughReplicas: Short = 19
  val NotEnoughReplicasAfterAppend: Short = 20
  val InvalidTopic: Short = 17
  val InvalidRequest: Short = 42
  val OutOfOrderSequenceNumber: Short = 45
  val InvalidProducerEpoch: Short = 47
  val InvalidTxnState: Short = 48
  val InvalidProducerIdMapping: Short = 49
  val InvalidTransactionTimeout: Short = 50
  val ConcurrentTransactions: Short = 51
  val TransactionalIdAuthorizationFailed: Short = 53
  val UnknownProducerId: Short = 59
  val UnsupportedVersion: Short = 35
  val TopicAlreadyExists: Short = 36
  val InvalidPartitions: Short = 37
  val InvalidReplicationFactor: Short = 38
  val NotController: Short = 41
  val CoordinatorLoadInProgress: Short = 14
  val CoordinatorNotAvailable: Short = 15
  val NotCoordinator: Short = 16
  val IllegalGeneration: Short = 22
  val InconsistentGroupProtocol: Short = 23
  val InvalidGroupId: Short = 24
  val UnknownMemberId: Short = 25
  val InvalidSessionTimeout: Short = 26
  val RebalanceInProgress: Short = 27
  val GroupAuthorizationFailed: Short = 30
  val MemberIdRequired: Short = 79
  val FencedLeaderEpoch: Short = 74
  val ProducerFenced: Short = 90
  val InvalidReplicaAssignment: Short = 39
  val ReassignmentInProgress: Short = 60
  val NoReassignmentInProgress: Short = 85
  val InconsistentClusterId: Short = 104
  val InvalidVoterKey: Short = 125
  val DuplicateVoter: Short = 126
  val VoterNotFound: Short = 127

final case class ApiVersion(apiKey: Short, minVersion: Short, maxVersion: Short)

object Compatibility:
  val supported: Vector[ApiVersion] = Vector(
    ApiVersion(ApiKey.Produce, 3, 3),
    ApiVersion(ApiKey.Fetch, 6, 6),
    ApiVersion(ApiKey.ListOffsets, 2, 2),
    ApiVersion(ApiKey.Metadata, 4, 4),
    ApiVersion(ApiKey.OffsetCommit, 7, 7),
    ApiVersion(ApiKey.OffsetFetch, 5, 5),
    ApiVersion(ApiKey.FindCoordinator, 2, 2),
    ApiVersion(ApiKey.JoinGroup, 5, 5),
    ApiVersion(ApiKey.Heartbeat, 3, 3),
    ApiVersion(ApiKey.LeaveGroup, 2, 2),
    ApiVersion(ApiKey.SyncGroup, 3, 3),
    ApiVersion(ApiKey.ApiVersions, 0, 4),
    ApiVersion(ApiKey.CreateTopics, 2, 2),
    ApiVersion(ApiKey.InitProducerId, 1, 1),
    ApiVersion(ApiKey.AddPartitionsToTxn, 1, 1),
    ApiVersion(ApiKey.AddOffsetsToTxn, 1, 1),
    ApiVersion(ApiKey.EndTxn, 1, 1),
    ApiVersion(ApiKey.TxnOffsetCommit, 2, 2),
    ApiVersion(ApiKey.AlterPartitionReassignments, 0, 0),
    ApiVersion(ApiKey.ListPartitionReassignments, 0, 0),
    ApiVersion(ApiKey.DescribeQuorum, 0, 2),
    ApiVersion(ApiKey.AddRaftVoter, 0, 1),
    ApiVersion(ApiKey.RemoveRaftVoter, 0, 0)
  )

  private val byKey = supported.map(version => version.apiKey -> version).toMap

  def accepts(apiKey: Short, version: Short): Boolean =
    byKey.get(apiKey).exists(range => version >= range.minVersion && version <= range.maxVersion)

  def isFlexibleRequest(apiKey: Short, version: Short): Boolean =
    (apiKey == ApiKey.ApiVersions && version >= 3) ||
      apiKey == ApiKey.AlterPartitionReassignments || apiKey == ApiKey.ListPartitionReassignments ||
      apiKey == ApiKey.DescribeQuorum || apiKey == ApiKey.AddRaftVoter || apiKey == ApiKey.RemoveRaftVoter

  // ApiVersions deliberately retains response header v0 even for flexible body versions.
  // All other flexible APIs use response header v1.
  def isFlexibleResponseHeader(apiKey: Short, version: Short): Boolean =
    apiKey == ApiKey.AlterPartitionReassignments || apiKey == ApiKey.ListPartitionReassignments ||
      apiKey == ApiKey.DescribeQuorum || apiKey == ApiKey.AddRaftVoter || apiKey == ApiKey.RemoveRaftVoter

final case class RequestHeader(
    apiKey: Short,
    apiVersion: Short,
    correlationId: Int,
    clientId: Option[String]
)

object RequestHeader:
  def decode(frame: Array[Byte]): (RequestHeader, ByteCursor) =
    val cursor = ByteCursor(frame)
    val apiKey = cursor.readShort()
    val version = cursor.readShort()
    val correlationId = cursor.readInt()
    val flexible = Compatibility.isFlexibleRequest(apiKey, version)
    // Header v2 adds tagged fields but deliberately keeps client_id as NULLABLE_STRING.
    val clientId = cursor.readNullableString()
    if flexible then cursor.skipTaggedFields()
    (RequestHeader(apiKey, version, correlationId, clientId), cursor)

object ResponseFrame:
  def encode(header: RequestHeader, body: Array[Byte]): Array[Byte] =
    val payload = ByteWriter(body.length + 16)
    payload.writeInt(header.correlationId)
    if Compatibility.isFlexibleResponseHeader(header.apiKey, header.apiVersion) then
      payload.writeEmptyTaggedFields()
    payload.writeBytes(body)
    val message = payload.result()
    ByteWriter(message.length + 4).writeInt(message.length).writeBytes(message).result()

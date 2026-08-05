package cascade.protocol

object ApiKey:
  val Produce: Short = 0
  val Fetch: Short = 1
  val ListOffsets: Short = 2
  val Metadata: Short = 3
  val ApiVersions: Short = 18
  val CreateTopics: Short = 19

object Errors:
  val None: Short = 0
  val UnknownTopicOrPartition: Short = 3
  val InvalidTopic: Short = 17
  val InvalidRequest: Short = 42
  val UnsupportedVersion: Short = 35
  val TopicAlreadyExists: Short = 36
  val InvalidPartitions: Short = 37
  val InvalidReplicationFactor: Short = 38

final case class ApiVersion(apiKey: Short, minVersion: Short, maxVersion: Short)

object Compatibility:
  val supported: Vector[ApiVersion] = Vector(
    ApiVersion(ApiKey.Produce, 3, 3),
    ApiVersion(ApiKey.Fetch, 6, 6),
    ApiVersion(ApiKey.ListOffsets, 2, 2),
    ApiVersion(ApiKey.Metadata, 4, 4),
    ApiVersion(ApiKey.ApiVersions, 0, 4),
    ApiVersion(ApiKey.CreateTopics, 2, 2)
  )

  private val byKey = supported.map(version => version.apiKey -> version).toMap

  def accepts(apiKey: Short, version: Short): Boolean =
    byKey.get(apiKey).exists(range => version >= range.minVersion && version <= range.maxVersion)

  def isFlexibleRequest(apiKey: Short, version: Short): Boolean =
    apiKey == ApiKey.ApiVersions && version >= 3

  // ApiVersions deliberately retains response header v0 even for flexible body versions.
  def isFlexibleResponseHeader(apiKey: Short, version: Short): Boolean = false

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

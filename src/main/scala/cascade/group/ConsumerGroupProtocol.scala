package cascade.group

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.collection.mutable

final case class ConsumerTopicId(mostSignificantBits: Long, leastSignificantBits: Long)

object ConsumerTopicId:
  def forName(topic: String): ConsumerTopicId =
    val uuid = UUID.nameUUIDFromBytes(s"cascade-topic:$topic".getBytes(StandardCharsets.UTF_8))
    ConsumerTopicId(uuid.getMostSignificantBits, uuid.getLeastSignificantBits)

final case class ConsumerTopicPartitions(topicId: ConsumerTopicId, partitions: Vector[Int])
final case class ConsumerHeartbeatCommand(
    groupId: String,
    memberId: String,
    memberEpoch: Int,
    instanceId: Option[String],
    rackId: Option[String],
    rebalanceTimeoutMillis: Int,
    subscribedTopicNames: Option[Vector[String]],
    serverAssignor: Option[String],
    ownedPartitions: Option[Vector[ConsumerTopicPartitions]],
    subscribedTopicRegex: Option[String] = None,
    clientId: String = "",
    clientHost: String = ""
)
final case class ConsumerHeartbeatResult(
    errorCode: Short,
    errorMessage: Option[String],
    memberId: Option[String],
    memberEpoch: Int,
    heartbeatIntervalMillis: Int,
    assignment: Option[Vector[ConsumerTopicPartitions]]
)

private[cascade] final case class OffsetDeleteResult(
    errorCode: Short,
    partitionErrors: Map[GroupOffsetKey, Short]
)

private[group] final class ConsumerMember(
    val memberId: String,
    var instanceId: Option[String],
    var rackId: Option[String],
    var rebalanceTimeoutMillis: Int,
    var subscriptions: Vector[String],
    var serverAssignor: String,
    var memberEpoch: Int,
    var lastHeartbeatMillis: Long,
    var assignment: Vector[ConsumerTopicPartitions],
    var subscribedTopicRegex: Option[String] = None,
    var clientId: String = "",
    var clientHost: String = "",
    var targetAssignment: Vector[ConsumerTopicPartitions] = Vector.empty
)

private[group] final class ManagedConsumerGroup:
  val members: mutable.LinkedHashMap[String, ConsumerMember] = mutable.LinkedHashMap.empty
  val partitionCounts: mutable.HashMap[String, Int] = mutable.HashMap.empty
  var groupEpoch = 0
  var assignmentEpoch = 0

private[cascade] final case class StoredConsumerMember(
    memberId: String,
    instanceId: Option[String],
    rackId: Option[String],
    rebalanceTimeoutMillis: Int,
    subscriptions: Vector[String],
    serverAssignor: String,
    memberEpoch: Int,
    lastHeartbeatMillis: Long,
    assignment: Vector[ConsumerTopicPartitions],
    subscribedTopicRegex: Option[String] = None,
    clientId: String = "",
    clientHost: String = "",
    targetAssignment: Vector[ConsumerTopicPartitions] = Vector.empty
)

private[cascade] final case class StoredConsumerGroup(
    groupId: String,
    groupEpoch: Int,
    members: Vector[StoredConsumerMember],
    assignmentEpoch: Int = 0
)

private[cascade] final case class ConsumerGroupDescriptionMember(
    memberId: String,
    instanceId: Option[String],
    rackId: Option[String],
    memberEpoch: Int,
    clientId: String,
    clientHost: String,
    subscribedTopicNames: Vector[String],
    subscribedTopicRegex: Option[String],
    assignment: Vector[ConsumerTopicPartitions],
    targetAssignment: Vector[ConsumerTopicPartitions]
)

private[cascade] final case class ConsumerGroupDescription(
    groupId: String,
    state: String,
    groupEpoch: Int,
    assignmentEpoch: Int,
    assignorName: String,
    members: Vector[ConsumerGroupDescriptionMember]
)

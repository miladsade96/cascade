package cascade.group

import cascade.coordinator.CoordinatorCheckpoint
import cascade.protocol.Errors
import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

final case class GroupProtocol(name: String, metadata: Array[Byte])
final case class JoinGroupCommand(
    groupId: String,
    sessionTimeoutMillis: Int,
    rebalanceTimeoutMillis: Int,
    memberId: String,
    groupInstanceId: Option[String],
    protocolType: String,
    protocols: Vector[GroupProtocol],
    clientId: String
)
final case class JoinedMember(memberId: String, groupInstanceId: Option[String], metadata: Array[Byte])
final case class JoinGroupResult(
    errorCode: Short,
    generationId: Int,
    protocolName: String,
    leaderId: String,
    memberId: String,
    members: Vector[JoinedMember]
)
final case class SyncGroupResult(errorCode: Short, assignment: Array[Byte])

private final class GroupMember(
    val memberId: String,
    var groupInstanceId: Option[String],
    var sessionTimeoutMillis: Int,
    var rebalanceTimeoutMillis: Int,
    var protocols: Vector[GroupProtocol],
    var clientId: String,
    var lastHeartbeatMillis: Long
):
  var assignment: Array[Byte] = Array.emptyByteArray

private final class ManagedGroup:
  val members: mutable.LinkedHashMap[String, GroupMember] = mutable.LinkedHashMap.empty
  val joined: mutable.HashSet[String] = mutable.HashSet.empty
  val pendingMemberIds: mutable.HashMap[String, Long] = mutable.HashMap.empty
  var phase: GroupStatus = GroupStatus.Empty
  var generationId = 0
  var leaderId = ""
  var protocolType = ""
  var protocolName = ""
  var rebalanceDeadlineMillis = 0L

/** Classic group coordinator with snapshot installation and durable local offsets. */
final class GroupCoordinator(
    offsetPath: Path,
    stateLock: Object = Object(),
    durableLocal: Boolean = true,
    scheduleExpiration: Boolean = true,
    offsetRetentionMillis: Long = -1L,
    journalCompactionBytes: Long = Long.MaxValue
) extends AutoCloseable:
  require(offsetRetentionMillis == -1L || offsetRetentionMillis > 0L, "offset retention must be -1 or positive")
  require(journalCompactionBytes >= 1024L, "offset journal compaction threshold must be at least 1 KiB")
  private val EmptyAssignment = Array.emptyByteArray
  private val closed = AtomicBoolean(false)
  private val groups = mutable.HashMap.empty[String, ManagedGroup]
  private val consumerGroups = mutable.HashMap.empty[String, ManagedConsumerGroup]
  private val offsets = OffsetStore(offsetPath)
  private var stateVersion = 0L
  private var checkpoint: CoordinatorCheckpoint = CoordinatorCheckpoint.Local
  private val expirationExecutor: Option[ScheduledExecutorService] = Option.when(scheduleExpiration) {
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-group-expirer").factory())
  }
  expirationExecutor.foreach(_.scheduleWithFixedDelay(() => expireNow(), 1L, 1L, TimeUnit.SECONDS): Unit)

  def attachCheckpoint(value: CoordinatorCheckpoint): Unit = stateLock.synchronized {
    checkpoint = value
  }

  def snapshotBytes: Array[Byte] = stateLock.synchronized(GroupCodec.encode(snapshotImage()))

  def installSnapshot(bytes: Vector[Byte]): Unit = stateLock.synchronized {
    installImage(if bytes.isEmpty then GroupImage.Empty else GroupCodec.decode(bytes.toArray))
  }

  /** KIP-848-style server-side membership and assignment, without the classic join/sync barrier. */
  def consumerHeartbeat(
      command: ConsumerHeartbeatCommand,
      partitionCount: String => Int,
      heartbeatIntervalMillis: Int = 5000
  ): ConsumerHeartbeatResult = stateLock.synchronized {
    def failure(code: Short, message: String, epoch: Int = command.memberEpoch): ConsumerHeartbeatResult =
      ConsumerHeartbeatResult(code, Some(message), Option(command.memberId).filter(_.nonEmpty), epoch, heartbeatIntervalMillis, None)

    if command.groupId.isEmpty then return failure(Errors.InvalidGroupId, "group ID must not be empty")
    if groups.get(command.groupId).exists(_.members.nonEmpty) then
      return failure(Errors.InconsistentGroupProtocol, "the group already uses the classic protocol")
    if command.serverAssignor.exists(name => name != "uniform" && name != "range") then
      return failure(Errors.UnsupportedAssignor, "supported server assignors are uniform and range")

    val group = consumerGroups.getOrElseUpdate(command.groupId, ManagedConsumerGroup())
    if command.memberEpoch == -1 then
      if command.memberId.isEmpty || group.members.remove(command.memberId).isEmpty then
        return failure(Errors.UnknownMemberId, "consumer member does not exist")
      rebalanceConsumerGroup(group, partitionCount)
      if !checkpointState() then return failure(Errors.CoordinatorNotAvailable, "coordinator checkpoint failed")
      return ConsumerHeartbeatResult(Errors.None, None, Some(command.memberId), -1, heartbeatIntervalMillis, None)

    val staticMember = command.instanceId.flatMap(instance =>
      group.members.valuesIterator.find(_.instanceId.contains(instance))
    )
    if command.memberEpoch == -2 then
      if command.instanceId.isEmpty then return failure(Errors.InvalidRequest, "static rejoin requires an instance ID")
      staticMember match
        case None => return failure(Errors.UnknownMemberId, "static consumer instance does not exist")
        case Some(existing) if command.memberId.nonEmpty && command.memberId != existing.memberId =>
          return failure(Errors.UnreleasedInstanceId, "consumer instance is owned by another member")
        case Some(existing) =>
          existing.lastHeartbeatMillis = System.currentTimeMillis()
          return ConsumerHeartbeatResult(
            Errors.None,
            None,
            Some(existing.memberId),
            existing.memberEpoch,
            heartbeatIntervalMillis,
            Some(existing.assignment)
          )

    val now = System.currentTimeMillis()
    val joining = command.memberEpoch == 0
    if joining then
      if command.rebalanceTimeoutMillis <= 0 || command.subscribedTopicNames.isEmpty ||
          command.ownedPartitions.forall(_.nonEmpty)
      then return failure(Errors.InvalidRequest, "initial heartbeat requires timeout, subscription, and an empty owned assignment")
      if staticMember.exists(_.memberId != command.memberId) then
        return failure(Errors.UnreleasedInstanceId, "consumer instance is owned by another member")
      val requestedAssignor = command.serverAssignor
        .orElse(group.members.headOption.map(_._2.serverAssignor))
        .getOrElse("uniform")
      if group.members.valuesIterator.exists(_.serverAssignor != requestedAssignor) then
        return failure(Errors.InconsistentGroupProtocol, "consumer group members must use one server assignor")
      val memberId = Option(command.memberId).filter(_.nonEmpty).getOrElse(newMemberId("consumer"))
      if group.members.contains(memberId) then return failure(Errors.FencedMemberEpoch, "member must rejoin with its current epoch")
      group.members.update(
        memberId,
        ConsumerMember(
          memberId,
          command.instanceId,
          command.rackId,
          command.rebalanceTimeoutMillis,
          command.subscribedTopicNames.getOrElse(Vector.empty).distinct.sorted,
          requestedAssignor,
          0,
          now,
          Vector.empty
        )
      )
      rebalanceConsumerGroup(group, partitionCount)
      if !checkpointState() then return failure(Errors.CoordinatorNotAvailable, "coordinator checkpoint failed")
      val member = group.members(memberId)
      ConsumerHeartbeatResult(Errors.None, None, Some(memberId), member.memberEpoch, heartbeatIntervalMillis, Some(member.assignment))
    else
      group.members.get(command.memberId) match
        case None => failure(Errors.UnknownMemberId, "consumer member does not exist")
        case Some(member) if command.memberEpoch > member.memberEpoch =>
          failure(Errors.FencedMemberEpoch, "consumer member epoch is ahead of the coordinator", member.memberEpoch)
        case Some(member) if command.memberEpoch < member.memberEpoch =>
          member.lastHeartbeatMillis = now
          ConsumerHeartbeatResult(
            Errors.None,
            None,
            Some(member.memberId),
            member.memberEpoch,
            heartbeatIntervalMillis,
            Some(member.assignment)
          )
        case Some(member) if command.instanceId.exists(id => !member.instanceId.contains(id)) =>
          failure(Errors.UnreleasedInstanceId, "consumer instance does not own this member", member.memberEpoch)
        case Some(member) =>
          if command.serverAssignor.exists(assignor => group.members.valuesIterator.exists(other => other.serverAssignor != assignor)) then
            return failure(Errors.InconsistentGroupProtocol, "consumer group members must use one server assignor", member.memberEpoch)
          val previousSubscriptions = member.subscriptions
          member.instanceId = command.instanceId.orElse(member.instanceId)
          member.rackId = command.rackId.orElse(member.rackId)
          if command.rebalanceTimeoutMillis >= 0 then member.rebalanceTimeoutMillis = command.rebalanceTimeoutMillis
          command.subscribedTopicNames.foreach(names => member.subscriptions = names.distinct.sorted)
          command.serverAssignor.foreach(member.serverAssignor = _)
          member.lastHeartbeatMillis = now
          val changed = previousSubscriptions != member.subscriptions
          if changed then rebalanceConsumerGroup(group, partitionCount)
          if changed && !checkpointState() then return failure(Errors.CoordinatorNotAvailable, "coordinator checkpoint failed")
          ConsumerHeartbeatResult(
            Errors.None,
            None,
            Some(member.memberId),
            member.memberEpoch,
            heartbeatIntervalMillis,
            Option.when(changed || !ownedMatches(command.ownedPartitions, member.assignment))(member.assignment)
          )
  }

  def join(command: JoinGroupCommand): JoinGroupResult = stateLock.synchronized {
    if command.groupId.isEmpty then return joinError(Errors.InvalidGroupId, command.memberId)
    if command.sessionTimeoutMillis <= 0 || command.rebalanceTimeoutMillis <= 0 then
      return joinError(Errors.InvalidSessionTimeout, command.memberId)
    if command.protocolType.isEmpty || command.protocols.isEmpty then
      return joinError(Errors.InconsistentGroupProtocol, command.memberId)

    val now = System.currentTimeMillis()
    val group = groups.getOrElseUpdate(command.groupId, ManagedGroup())
    removeExpiredPendingIds(group, now)

    val claimedInstanceMember = command.groupInstanceId.flatMap(instance =>
      group.members.valuesIterator.find(_.groupInstanceId.contains(instance))
    )
    if command.memberId.nonEmpty && claimedInstanceMember.exists(_.memberId != command.memberId) then
      return joinError(Errors.FencedInstanceId, command.memberId)
    group.members.get(command.memberId) match
      case Some(member) if member.groupInstanceId != command.groupInstanceId =>
        return joinError(Errors.FencedInstanceId, command.memberId)
      case _ => ()

    val effectiveCommand =
      if command.memberId.isEmpty && command.groupInstanceId.nonEmpty then
        claimedInstanceMember.foreach(member => group.members.remove(member.memberId): Unit)
        command.copy(memberId = newMemberId(command.clientId))
      else command

    if effectiveCommand.memberId.isEmpty then
      val assignedId = newMemberId(effectiveCommand.clientId)
      group.pendingMemberIds.update(assignedId, now + command.sessionTimeoutMillis.toLong)
      if !checkpointState() then return joinError(Errors.CoordinatorNotAvailable, assignedId)
      return joinError(Errors.MemberIdRequired, assignedId)

    val existing = group.members.get(effectiveCommand.memberId)
    val staticAdmission = effectiveCommand.groupInstanceId.nonEmpty && command.memberId.isEmpty
    if existing.isEmpty && !staticAdmission && group.pendingMemberIds.remove(effectiveCommand.memberId).isEmpty then
      return joinError(Errors.UnknownMemberId, effectiveCommand.memberId)

    val candidate = GroupMember(
      effectiveCommand.memberId,
      effectiveCommand.groupInstanceId,
      effectiveCommand.sessionTimeoutMillis,
      effectiveCommand.rebalanceTimeoutMillis,
      effectiveCommand.protocols,
      effectiveCommand.clientId,
      now
    )
    val peers = group.members.valuesIterator.filterNot(_.memberId == command.memberId).toVector
    if peers.nonEmpty && (group.protocolType != effectiveCommand.protocolType || commonProtocol(peers :+ candidate).isEmpty) then
      return joinError(Errors.InconsistentGroupProtocol, effectiveCommand.memberId)

    val beginsRebalance = group.phase match
      case GroupStatus.Empty => true
      case GroupStatus.Stable | GroupStatus.CompletingRebalance => true
      case GroupStatus.PreparingRebalance => false
    if beginsRebalance then beginRebalance(group, now)

    existing match
      case Some(member) =>
        member.groupInstanceId = effectiveCommand.groupInstanceId
        member.sessionTimeoutMillis = effectiveCommand.sessionTimeoutMillis
        member.rebalanceTimeoutMillis = effectiveCommand.rebalanceTimeoutMillis
        member.protocols = effectiveCommand.protocols
        member.clientId = effectiveCommand.clientId
        member.lastHeartbeatMillis = now
      case None => group.members.update(effectiveCommand.memberId, candidate)
    group.protocolType = effectiveCommand.protocolType
    group.joined += effectiveCommand.memberId

    if group.joined.size == group.members.size then completeJoin(group)
    else awaitJoin(group, effectiveCommand.memberId)

    if !checkpointState() then return joinError(Errors.CoordinatorNotAvailable, effectiveCommand.memberId)

    group.members.get(effectiveCommand.memberId) match
      case None => joinError(Errors.UnknownMemberId, effectiveCommand.memberId)
      case Some(member) if group.phase == GroupStatus.PreparingRebalance =>
        joinError(Errors.RebalanceInProgress, member.memberId)
      case Some(member) => successfulJoin(group, member)
  }

  def sync(
      groupId: String,
      generationId: Int,
      memberId: String,
      assignments: Vector[(String, Array[Byte])]
  ): SyncGroupResult = sync(groupId, generationId, memberId, None, assignments)

  def sync(
      groupId: String,
      generationId: Int,
      memberId: String,
      groupInstanceId: Option[String],
      assignments: Vector[(String, Array[Byte])]
  ): SyncGroupResult = stateLock.synchronized {
    groups.get(groupId) match
      case None => SyncGroupResult(Errors.UnknownMemberId, EmptyAssignment)
      case Some(group) =>
        validateMember(group, generationId, memberId, groupInstanceId) match
          case error if error != Errors.None => SyncGroupResult(error, EmptyAssignment)
          case _ if group.phase == GroupStatus.PreparingRebalance =>
            SyncGroupResult(Errors.RebalanceInProgress, EmptyAssignment)
          case _ =>
            var changed = false
            if group.phase == GroupStatus.CompletingRebalance && assignments.nonEmpty then
              if memberId != group.leaderId then return SyncGroupResult(Errors.IllegalGeneration, EmptyAssignment)
              val supplied = assignments.toMap
              group.members.valuesIterator.foreach { member =>
                member.assignment = supplied.getOrElse(member.memberId, EmptyAssignment)
              }
              group.phase = GroupStatus.Stable
              changed = true
              stateLock.notifyAll()
            else if group.phase == GroupStatus.CompletingRebalance then awaitSync(group, memberId)

            if changed && !checkpointState() then
              return SyncGroupResult(Errors.CoordinatorNotAvailable, EmptyAssignment)

            group.members.get(memberId) match
              case Some(member) if group.phase == GroupStatus.Stable =>
                member.lastHeartbeatMillis = System.currentTimeMillis()
                SyncGroupResult(Errors.None, member.assignment)
              case Some(_) => SyncGroupResult(Errors.RebalanceInProgress, EmptyAssignment)
              case None    => SyncGroupResult(Errors.UnknownMemberId, EmptyAssignment)
  }

  def heartbeat(groupId: String, generationId: Int, memberId: String, groupInstanceId: Option[String] = None): Short = stateLock.synchronized {
    groups.get(groupId) match
      case None => Errors.UnknownMemberId
      case Some(group) =>
        validateMember(group, generationId, memberId, groupInstanceId) match
          case error if error != Errors.None => error
          case _ if group.phase != GroupStatus.Stable => Errors.RebalanceInProgress
          case _ =>
            group.members(memberId).lastHeartbeatMillis = System.currentTimeMillis()
            Errors.None
  }

  def leave(groupId: String, memberId: String): Short = stateLock.synchronized {
    groups.get(groupId) match
      case None => Errors.UnknownMemberId
      case Some(group) if group.members.remove(memberId).isEmpty => Errors.UnknownMemberId
      case Some(group) =>
        group.joined -= memberId
        if group.members.isEmpty then resetEmpty(group)
        else if group.phase == GroupStatus.PreparingRebalance then
          if group.joined.size == group.members.size then completeJoin(group)
        else beginRebalance(group, System.currentTimeMillis())
        stateLock.notifyAll()
        if checkpointState() then Errors.None else Errors.CoordinatorNotAvailable
  }

  def commitOffsets(
      groupId: String,
      generationId: Int,
      memberId: String,
      values: Vector[OffsetCommitValue]
  ): Short = commitOffsets(groupId, generationId, memberId, None, values)

  def commitOffsets(
      groupId: String,
      generationId: Int,
      memberId: String,
      groupInstanceId: Option[String],
      values: Vector[OffsetCommitValue]
  ): Short = stateLock.synchronized {
    val validation =
      if groupId.isEmpty then Errors.InvalidGroupId
      else if values.exists(_.key.groupId != groupId) then Errors.InvalidRequest
      else if generationId < 0 then Errors.None
      else groups.get(groupId).map(validateMember(_, generationId, memberId, groupInstanceId)).getOrElse(Errors.UnknownMemberId)
    if validation == Errors.None then
      offsets.commit(values, durableLocal)
      if durableLocal && offsets.journalSize >= journalCompactionBytes then offsets.compact()
      if !checkpointState() then return Errors.CoordinatorNotAvailable
    validation
  }

  def fetchOffset(key: GroupOffsetKey): Option[CommittedOffset] = offsets.get(key)

  def allOffsets(groupId: String): Vector[(GroupOffsetKey, CommittedOffset)] = offsets.all(groupId)

  /** Stages offsets inside a caller-owned combined coordinator checkpoint. */
  private[cascade] def stageReplicatedOffsets(values: Vector[OffsetCommitValue]): Unit = stateLock.synchronized {
    offsets.commit(values, durableLocal)
    stateVersion = Math.addExact(stateVersion, 1L)
  }

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      expirationExecutor.foreach { executor =>
        executor.shutdownNow(): Unit
        executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
      }
      offsets.close()

  private def successfulJoin(group: ManagedGroup, member: GroupMember): JoinGroupResult =
    val members =
      if member.memberId == group.leaderId then
        group.members.valuesIterator.map { value =>
          val metadata = value.protocols.find(_.name == group.protocolName).map(_.metadata).getOrElse(Array.emptyByteArray)
          JoinedMember(value.memberId, value.groupInstanceId, metadata)
        }.toVector
      else Vector.empty
    JoinGroupResult(Errors.None, group.generationId, group.protocolName, group.leaderId, member.memberId, members)

  private def joinError(error: Short, memberId: String): JoinGroupResult =
    JoinGroupResult(error, -1, "", "", memberId, Vector.empty)

  private def beginRebalance(group: ManagedGroup, now: Long): Unit =
    group.phase = GroupStatus.PreparingRebalance
    group.joined.clear()
    group.members.valuesIterator.foreach(_.assignment = EmptyAssignment)
    val timeoutMillis = group.members.valuesIterator.map(_.rebalanceTimeoutMillis.toLong).maxOption.getOrElse(1000L)
    group.rebalanceDeadlineMillis = now + timeoutMillis
    stateLock.notifyAll()

  private def completeJoin(group: ManagedGroup): Unit =
    if group.members.isEmpty then resetEmpty(group)
    else
      group.generationId = Math.addExact(group.generationId, 1)
      if !group.members.contains(group.leaderId) then group.leaderId = group.members.head._1
      group.protocolName = commonProtocol(group.members.valuesIterator.toVector).getOrElse("")
      group.phase = GroupStatus.CompletingRebalance
      stateLock.notifyAll()

  private def awaitJoin(group: ManagedGroup, memberId: String): Unit =
    var remainingMillis = group.rebalanceDeadlineMillis - System.currentTimeMillis()
    while group.phase == GroupStatus.PreparingRebalance && group.members.contains(memberId) && remainingMillis > 0L do
      stateLock.wait(math.max(1L, remainingMillis))
      remainingMillis = group.rebalanceDeadlineMillis - System.currentTimeMillis()
    if group.phase == GroupStatus.PreparingRebalance then
      group.members.keysIterator.filterNot(group.joined.contains).toVector.foreach(group.members.remove)
      completeJoin(group)

  private def awaitSync(group: ManagedGroup, memberId: String): Unit =
    var remainingMillis = group.rebalanceDeadlineMillis - System.currentTimeMillis()
    while group.phase == GroupStatus.CompletingRebalance && group.members.contains(memberId) && remainingMillis > 0L do
      stateLock.wait(math.max(1L, remainingMillis))
      remainingMillis = group.rebalanceDeadlineMillis - System.currentTimeMillis()

  private def validateMember(
      group: ManagedGroup,
      generationId: Int,
      memberId: String,
      groupInstanceId: Option[String]
  ): Short =
    group.members.get(memberId) match
      case None if groupInstanceId.exists(instance => group.members.valuesIterator.exists(_.groupInstanceId.contains(instance))) =>
        Errors.FencedInstanceId
      case None => Errors.UnknownMemberId
      case Some(member) if member.groupInstanceId != groupInstanceId => Errors.FencedInstanceId
      case Some(_) if generationId != group.generationId => Errors.IllegalGeneration
      case Some(_) => Errors.None

  private def commonProtocol(members: Vector[GroupMember]): Option[String] =
    members.headOption.flatMap { leader =>
      leader.protocols.iterator.map(_.name).find(name => members.forall(_.protocols.exists(_.name == name)))
    }

  def expireNow(nowMillis: Long = System.currentTimeMillis()): Unit = stateLock.synchronized {
    val now = nowMillis
    var changed = false
    groups.valuesIterator.foreach { group =>
      val pendingBefore = group.pendingMemberIds.size
      removeExpiredPendingIds(group, now)
      changed ||= group.pendingMemberIds.size != pendingBefore
      if group.phase == GroupStatus.Stable then
        val expired = group.members.valuesIterator
          .filter(member => now - member.lastHeartbeatMillis >= member.sessionTimeoutMillis.toLong)
          .map(_.memberId)
          .toVector
        if expired.nonEmpty then
          changed = true
          expired.foreach(group.members.remove)
          if group.members.isEmpty then resetEmpty(group) else beginRebalance(group, now)
          stateLock.notifyAll()
    }
    consumerGroups.valuesIterator.foreach { group =>
      val expired = group.members.valuesIterator
        .filter(member => now - member.lastHeartbeatMillis >= 45_000L)
        .map(_.memberId)
        .toVector
      if expired.nonEmpty then
        changed = true
        expired.foreach(group.members.remove)
        rebalanceConsumerGroup(group, _ => 0)
    }
    if offsetRetentionMillis > 0L then
      val expiredOffsets = offsets.expireBefore(now - offsetRetentionMillis, durableLocal)
      changed ||= expiredOffsets.nonEmpty
    if changed then checkpointState(): Unit
  }

  private def removeExpiredPendingIds(group: ManagedGroup, now: Long): Unit =
    group.pendingMemberIds.iterator.filter(_._2 <= now).map(_._1).toVector.foreach(group.pendingMemberIds.remove)

  private def resetEmpty(group: ManagedGroup): Unit =
    group.phase = GroupStatus.Empty
    group.joined.clear()
    group.leaderId = ""
    group.protocolName = ""

  private def newMemberId(clientId: String): String =
    val prefix = if clientId.isEmpty then "consumer" else clientId
    s"$prefix-${UUID.randomUUID()}"

  private def checkpointState(): Boolean =
    stateVersion = Math.addExact(stateVersion, 1L)
    checkpoint.commit()

  private def snapshotImage(): GroupImage =
    val storedGroups = groups.iterator.toVector.sortBy(_._1).map { case (groupId, group) =>
      StoredGroup(
        groupId,
        group.phase,
        group.generationId,
        group.leaderId,
        group.protocolType,
        group.protocolName,
        group.rebalanceDeadlineMillis,
        group.members.valuesIterator.map { member =>
          StoredMember(
            member.memberId,
            member.groupInstanceId,
            member.sessionTimeoutMillis,
            member.rebalanceTimeoutMillis,
            member.protocols.map(protocol => StoredProtocol(protocol.name, protocol.metadata.toVector)),
            member.clientId,
            member.lastHeartbeatMillis,
            member.assignment.toVector
          )
        }.toVector,
        group.joined.toVector.sorted,
        group.pendingMemberIds.toVector.sortBy(_._1)
      )
    }
    val storedConsumers = consumerGroups.iterator.toVector.sortBy(_._1).map { case (groupId, group) =>
      StoredConsumerGroup(
        groupId,
        group.groupEpoch,
        group.members.valuesIterator.map { member =>
          StoredConsumerMember(
            member.memberId,
            member.instanceId,
            member.rackId,
            member.rebalanceTimeoutMillis,
            member.subscriptions,
            member.serverAssignor,
            member.memberEpoch,
            member.lastHeartbeatMillis,
            member.assignment
          )
        }.toVector
      )
    }
    GroupImage(stateVersion, storedGroups, offsets.entries, storedConsumers)

  private def installImage(image: GroupImage): Unit =
    groups.clear()
    consumerGroups.clear()
    val installedAtMillis = System.currentTimeMillis()
    image.groups.foreach { stored =>
      val group = ManagedGroup()
      group.phase = stored.status
      group.generationId = stored.generationId
      group.leaderId = stored.leaderId
      group.protocolType = stored.protocolType
      group.protocolName = stored.protocolName
      group.rebalanceDeadlineMillis = stored.rebalanceDeadlineMillis
      stored.members.foreach { value =>
        val member = GroupMember(
          value.memberId,
          value.groupInstanceId,
          value.sessionTimeoutMillis,
          value.rebalanceTimeoutMillis,
          value.protocols.map(protocol => GroupProtocol(protocol.name, protocol.metadata.toArray)),
          value.clientId,
          installedAtMillis
        )
        member.assignment = value.assignment.toArray
        group.members.update(member.memberId, member)
      }
      group.joined ++= stored.joined
      group.pendingMemberIds ++= stored.pendingMemberIds
      groups.update(stored.groupId, group)
    }
    offsets.install(image.offsets)
    image.consumerGroups.foreach { stored =>
      val group = ManagedConsumerGroup()
      group.groupEpoch = stored.groupEpoch
      stored.members.foreach { value =>
        group.members.update(
          value.memberId,
          ConsumerMember(
            value.memberId,
            value.instanceId,
            value.rackId,
            value.rebalanceTimeoutMillis,
            value.subscriptions,
            value.serverAssignor,
            value.memberEpoch,
            installedAtMillis,
            value.assignment
          )
        )
      }
      stored.members.iterator.flatMap { member =>
        member.subscriptions.iterator.flatMap { topic =>
          val topicId = ConsumerTopicId.forName(topic)
          member.assignment.iterator
            .filter(_.topicId == topicId)
            .flatMap(_.partitions.maxOption.map(_ + 1))
            .map(topic -> _)
        }
      }.foreach { case (topic, count) =>
        group.partitionCounts.update(topic, math.max(group.partitionCounts.getOrElse(topic, 0), count))
      }
      consumerGroups.update(stored.groupId, group)
    }
    stateVersion = image.version
    stateLock.notifyAll()

  private def rebalanceConsumerGroup(group: ManagedConsumerGroup, partitionCount: String => Int): Unit =
    group.groupEpoch = Math.addExact(group.groupEpoch, 1)
    val assignments = mutable.HashMap.from(group.members.keysIterator.map(_ -> mutable.ArrayBuffer.empty[ConsumerTopicPartitions]))
    val topicNames = group.members.valuesIterator.flatMap(_.subscriptions).toSet.toVector.sorted
    topicNames.foreach { topic =>
      val subscribers = group.members.valuesIterator.filter(_.subscriptions.contains(topic)).toVector.sortBy(_.memberId)
      if subscribers.nonEmpty then
        val observedCount = math.max(0, partitionCount(topic))
        if observedCount > 0 then group.partitionCounts.update(topic, observedCount)
        val effectiveCount = if observedCount > 0 then observedCount else group.partitionCounts.getOrElse(topic, 0)
        val buffers = mutable.HashMap.from(subscribers.map(member => member.memberId -> mutable.ArrayBuffer.empty[Int]))
        if subscribers.head.serverAssignor == "range" then
          val width = effectiveCount / subscribers.size
          val remainder = effectiveCount % subscribers.size
          subscribers.indices.foreach { index =>
            val start = index * width + math.min(index, remainder)
            val size = width + Option.when(index < remainder)(1).getOrElse(0)
            (start until start + size).foreach(buffers(subscribers(index).memberId) += _)
          }
        else
          (0 until effectiveCount).foreach { partition =>
            buffers(subscribers(partition % subscribers.size).memberId) += partition
          }
        val topicId = ConsumerTopicId.forName(topic)
        subscribers.foreach { member =>
          val partitions = buffers(member.memberId).toVector
          if partitions.nonEmpty then assignments(member.memberId) += ConsumerTopicPartitions(topicId, partitions)
        }
    }
    group.members.valuesIterator.foreach { member =>
      member.memberEpoch = group.groupEpoch
      member.assignment = assignments(member.memberId).toVector
    }

  private def ownedMatches(
      owned: Option[Vector[ConsumerTopicPartitions]],
      assigned: Vector[ConsumerTopicPartitions]
  ): Boolean = owned.exists(_.sortBy(value => (value.topicId.mostSignificantBits, value.topicId.leastSignificantBits)) ==
    assigned.sortBy(value => (value.topicId.mostSignificantBits, value.topicId.leastSignificantBits)))

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
    scheduleExpiration: Boolean = true
) extends AutoCloseable:
  private val EmptyAssignment = Array.emptyByteArray
  private val closed = AtomicBoolean(false)
  private val groups = mutable.HashMap.empty[String, ManagedGroup]
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

  def join(command: JoinGroupCommand): JoinGroupResult = stateLock.synchronized {
    if command.groupId.isEmpty then return joinError(Errors.InvalidGroupId, command.memberId)
    if command.sessionTimeoutMillis <= 0 || command.rebalanceTimeoutMillis <= 0 then
      return joinError(Errors.InvalidSessionTimeout, command.memberId)
    if command.protocolType.isEmpty || command.protocols.isEmpty then
      return joinError(Errors.InconsistentGroupProtocol, command.memberId)

    val now = System.currentTimeMillis()
    val group = groups.getOrElseUpdate(command.groupId, ManagedGroup())
    removeExpiredPendingIds(group, now)

    if command.memberId.isEmpty then
      val assignedId = newMemberId(command.clientId)
      group.pendingMemberIds.update(assignedId, now + command.sessionTimeoutMillis.toLong)
      if !checkpointState() then return joinError(Errors.CoordinatorNotAvailable, assignedId)
      return joinError(Errors.MemberIdRequired, assignedId)

    val existing = group.members.get(command.memberId)
    if existing.isEmpty && group.pendingMemberIds.remove(command.memberId).isEmpty then
      return joinError(Errors.UnknownMemberId, command.memberId)

    val candidate = GroupMember(
      command.memberId,
      command.groupInstanceId,
      command.sessionTimeoutMillis,
      command.rebalanceTimeoutMillis,
      command.protocols,
      command.clientId,
      now
    )
    val peers = group.members.valuesIterator.filterNot(_.memberId == command.memberId).toVector
    if peers.nonEmpty && (group.protocolType != command.protocolType || commonProtocol(peers :+ candidate).isEmpty) then
      return joinError(Errors.InconsistentGroupProtocol, command.memberId)

    val beginsRebalance = group.phase match
      case GroupStatus.Empty => true
      case GroupStatus.Stable | GroupStatus.CompletingRebalance => true
      case GroupStatus.PreparingRebalance => false
    if beginsRebalance then beginRebalance(group, now)

    existing match
      case Some(member) =>
        member.groupInstanceId = command.groupInstanceId
        member.sessionTimeoutMillis = command.sessionTimeoutMillis
        member.rebalanceTimeoutMillis = command.rebalanceTimeoutMillis
        member.protocols = command.protocols
        member.clientId = command.clientId
        member.lastHeartbeatMillis = now
      case None => group.members.update(command.memberId, candidate)
    group.protocolType = command.protocolType
    group.joined += command.memberId

    if group.joined.size == group.members.size then completeJoin(group)
    else awaitJoin(group, command.memberId)

    if !checkpointState() then return joinError(Errors.CoordinatorNotAvailable, command.memberId)

    group.members.get(command.memberId) match
      case None => joinError(Errors.UnknownMemberId, command.memberId)
      case Some(member) if group.phase == GroupStatus.PreparingRebalance =>
        joinError(Errors.RebalanceInProgress, member.memberId)
      case Some(member) => successfulJoin(group, member)
  }

  def sync(
      groupId: String,
      generationId: Int,
      memberId: String,
      assignments: Vector[(String, Array[Byte])]
  ): SyncGroupResult = stateLock.synchronized {
    groups.get(groupId) match
      case None => SyncGroupResult(Errors.UnknownMemberId, EmptyAssignment)
      case Some(group) =>
        validateMember(group, generationId, memberId) match
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

  def heartbeat(groupId: String, generationId: Int, memberId: String): Short = stateLock.synchronized {
    groups.get(groupId) match
      case None => Errors.UnknownMemberId
      case Some(group) =>
        validateMember(group, generationId, memberId) match
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
  ): Short =
    val validation = stateLock.synchronized {
      if groupId.isEmpty then Errors.InvalidGroupId
      else if generationId < 0 then Errors.None
      else groups.get(groupId).map(validateMember(_, generationId, memberId)).getOrElse(Errors.UnknownMemberId)
    }
    if validation == Errors.None then stateLock.synchronized {
      offsets.commit(values, durableLocal)
      if !checkpointState() then return Errors.CoordinatorNotAvailable
    }
    validation

  def fetchOffset(key: GroupOffsetKey): Option[CommittedOffset] = offsets.get(key)

  def allOffsets(groupId: String): Vector[(GroupOffsetKey, CommittedOffset)] = offsets.all(groupId)

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

  private def validateMember(group: ManagedGroup, generationId: Int, memberId: String): Short =
    if !group.members.contains(memberId) then Errors.UnknownMemberId
    else if generationId != group.generationId then Errors.IllegalGeneration
    else Errors.None

  private def commonProtocol(members: Vector[GroupMember]): Option[String] =
    members.headOption.flatMap { leader =>
      leader.protocols.iterator.map(_.name).find(name => members.forall(_.protocols.exists(_.name == name)))
    }

  def expireNow(): Unit = stateLock.synchronized {
    val now = System.currentTimeMillis()
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
    GroupImage(stateVersion, storedGroups, offsets.entries)

  private def installImage(image: GroupImage): Unit =
    groups.clear()
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
          value.lastHeartbeatMillis
        )
        member.assignment = value.assignment.toArray
        group.members.update(member.memberId, member)
      }
      group.joined ++= stored.joined
      group.pendingMemberIds ++= stored.pendingMemberIds
      groups.update(stored.groupId, group)
    }
    offsets.install(image.offsets)
    stateVersion = image.version
    stateLock.notifyAll()

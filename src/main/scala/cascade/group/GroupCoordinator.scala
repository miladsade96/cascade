package cascade.group

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

private enum GroupPhase:
  case Empty, PreparingRebalance, CompletingRebalance, Stable

private final class GroupMember(
    val memberId: String,
    var groupInstanceId: Option[String],
    var sessionTimeoutMillis: Int,
    var rebalanceTimeoutMillis: Int,
    var protocols: Vector[GroupProtocol],
    var clientId: String,
    var lastHeartbeatNanos: Long
):
  var assignment: Array[Byte] = Array.emptyByteArray

private final class ManagedGroup:
  val members: mutable.LinkedHashMap[String, GroupMember] = mutable.LinkedHashMap.empty
  val joined: mutable.HashSet[String] = mutable.HashSet.empty
  val pendingMemberIds: mutable.HashMap[String, Long] = mutable.HashMap.empty
  var phase: GroupPhase = GroupPhase.Empty
  var generationId = 0
  var leaderId = ""
  var protocolType = ""
  var protocolName = ""
  var rebalanceDeadlineNanos = 0L

/** Single-node classic group coordinator with durable committed offsets. */
final class GroupCoordinator(offsetPath: Path) extends AutoCloseable:
  private val EmptyAssignment = Array.emptyByteArray
  private val closed = AtomicBoolean(false)
  private val groups = mutable.HashMap.empty[String, ManagedGroup]
  private val offsets = OffsetStore(offsetPath)
  private val expirationExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-group-expirer").factory())
  expirationExecutor.scheduleWithFixedDelay(() => expireMembers(), 1L, 1L, TimeUnit.SECONDS): Unit

  def join(command: JoinGroupCommand): JoinGroupResult = synchronized {
    if command.groupId.isEmpty then return joinError(Errors.InvalidGroupId, command.memberId)
    if command.sessionTimeoutMillis <= 0 || command.rebalanceTimeoutMillis <= 0 then
      return joinError(Errors.InvalidSessionTimeout, command.memberId)
    if command.protocolType.isEmpty || command.protocols.isEmpty then
      return joinError(Errors.InconsistentGroupProtocol, command.memberId)

    val now = System.nanoTime()
    val group = groups.getOrElseUpdate(command.groupId, ManagedGroup())
    removeExpiredPendingIds(group, now)

    if command.memberId.isEmpty then
      val assignedId = newMemberId(command.clientId)
      group.pendingMemberIds.update(assignedId, now + command.sessionTimeoutMillis.toLong * 1_000_000L)
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
      case GroupPhase.Empty => true
      case GroupPhase.Stable | GroupPhase.CompletingRebalance => true
      case GroupPhase.PreparingRebalance => false
    if beginsRebalance then beginRebalance(group, now)

    existing match
      case Some(member) =>
        member.groupInstanceId = command.groupInstanceId
        member.sessionTimeoutMillis = command.sessionTimeoutMillis
        member.rebalanceTimeoutMillis = command.rebalanceTimeoutMillis
        member.protocols = command.protocols
        member.clientId = command.clientId
        member.lastHeartbeatNanos = now
      case None => group.members.update(command.memberId, candidate)
    group.protocolType = command.protocolType
    group.joined += command.memberId

    if group.joined.size == group.members.size then completeJoin(group)
    else awaitJoin(group, command.memberId)

    group.members.get(command.memberId) match
      case None => joinError(Errors.UnknownMemberId, command.memberId)
      case Some(member) if group.phase == GroupPhase.PreparingRebalance =>
        joinError(Errors.RebalanceInProgress, member.memberId)
      case Some(member) => successfulJoin(group, member)
  }

  def sync(
      groupId: String,
      generationId: Int,
      memberId: String,
      assignments: Vector[(String, Array[Byte])]
  ): SyncGroupResult = synchronized {
    groups.get(groupId) match
      case None => SyncGroupResult(Errors.UnknownMemberId, EmptyAssignment)
      case Some(group) =>
        validateMember(group, generationId, memberId) match
          case error if error != Errors.None => SyncGroupResult(error, EmptyAssignment)
          case _ if group.phase == GroupPhase.PreparingRebalance =>
            SyncGroupResult(Errors.RebalanceInProgress, EmptyAssignment)
          case _ =>
            if group.phase == GroupPhase.CompletingRebalance && assignments.nonEmpty then
              if memberId != group.leaderId then return SyncGroupResult(Errors.IllegalGeneration, EmptyAssignment)
              val supplied = assignments.toMap
              group.members.valuesIterator.foreach { member =>
                member.assignment = supplied.getOrElse(member.memberId, EmptyAssignment)
              }
              group.phase = GroupPhase.Stable
              notifyAll()
            else if group.phase == GroupPhase.CompletingRebalance then awaitSync(group, memberId)

            group.members.get(memberId) match
              case Some(member) if group.phase == GroupPhase.Stable =>
                member.lastHeartbeatNanos = System.nanoTime()
                SyncGroupResult(Errors.None, member.assignment)
              case Some(_) => SyncGroupResult(Errors.RebalanceInProgress, EmptyAssignment)
              case None    => SyncGroupResult(Errors.UnknownMemberId, EmptyAssignment)
  }

  def heartbeat(groupId: String, generationId: Int, memberId: String): Short = synchronized {
    groups.get(groupId) match
      case None => Errors.UnknownMemberId
      case Some(group) =>
        validateMember(group, generationId, memberId) match
          case error if error != Errors.None => error
          case _ if group.phase != GroupPhase.Stable => Errors.RebalanceInProgress
          case _ =>
            group.members(memberId).lastHeartbeatNanos = System.nanoTime()
            Errors.None
  }

  def leave(groupId: String, memberId: String): Short = synchronized {
    groups.get(groupId) match
      case None => Errors.UnknownMemberId
      case Some(group) if group.members.remove(memberId).isEmpty => Errors.UnknownMemberId
      case Some(group) =>
        group.joined -= memberId
        if group.members.isEmpty then resetEmpty(group)
        else if group.phase == GroupPhase.PreparingRebalance then
          if group.joined.size == group.members.size then completeJoin(group)
        else beginRebalance(group, System.nanoTime())
        notifyAll()
        Errors.None
  }

  def commitOffsets(
      groupId: String,
      generationId: Int,
      memberId: String,
      values: Vector[OffsetCommitValue]
  ): Short =
    val validation = synchronized {
      if groupId.isEmpty then Errors.InvalidGroupId
      else if generationId < 0 then Errors.None
      else groups.get(groupId).map(validateMember(_, generationId, memberId)).getOrElse(Errors.UnknownMemberId)
    }
    if validation == Errors.None then offsets.commit(values)
    validation

  def fetchOffset(key: GroupOffsetKey): Option[CommittedOffset] = offsets.get(key)

  def allOffsets(groupId: String): Vector[(GroupOffsetKey, CommittedOffset)] = offsets.all(groupId)

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      expirationExecutor.shutdownNow(): Unit
      expirationExecutor.awaitTermination(5L, TimeUnit.SECONDS): Unit
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
    group.phase = GroupPhase.PreparingRebalance
    group.joined.clear()
    group.members.valuesIterator.foreach(_.assignment = EmptyAssignment)
    val timeoutMillis = group.members.valuesIterator.map(_.rebalanceTimeoutMillis.toLong).maxOption.getOrElse(1000L)
    group.rebalanceDeadlineNanos = now + timeoutMillis * 1_000_000L
    notifyAll()

  private def completeJoin(group: ManagedGroup): Unit =
    if group.members.isEmpty then resetEmpty(group)
    else
      group.generationId = Math.addExact(group.generationId, 1)
      if !group.members.contains(group.leaderId) then group.leaderId = group.members.head._1
      group.protocolName = commonProtocol(group.members.valuesIterator.toVector).getOrElse("")
      group.phase = GroupPhase.CompletingRebalance
      notifyAll()

  private def awaitJoin(group: ManagedGroup, memberId: String): Unit =
    var remainingNanos = group.rebalanceDeadlineNanos - System.nanoTime()
    while group.phase == GroupPhase.PreparingRebalance && group.members.contains(memberId) && remainingNanos > 0L do
      wait(math.max(1L, remainingNanos / 1_000_000L))
      remainingNanos = group.rebalanceDeadlineNanos - System.nanoTime()
    if group.phase == GroupPhase.PreparingRebalance then
      group.members.keysIterator.filterNot(group.joined.contains).toVector.foreach(group.members.remove)
      completeJoin(group)

  private def awaitSync(group: ManagedGroup, memberId: String): Unit =
    var remainingNanos = group.rebalanceDeadlineNanos - System.nanoTime()
    while group.phase == GroupPhase.CompletingRebalance && group.members.contains(memberId) && remainingNanos > 0L do
      wait(math.max(1L, remainingNanos / 1_000_000L))
      remainingNanos = group.rebalanceDeadlineNanos - System.nanoTime()

  private def validateMember(group: ManagedGroup, generationId: Int, memberId: String): Short =
    if !group.members.contains(memberId) then Errors.UnknownMemberId
    else if generationId != group.generationId then Errors.IllegalGeneration
    else Errors.None

  private def commonProtocol(members: Vector[GroupMember]): Option[String] =
    members.headOption.flatMap { leader =>
      leader.protocols.iterator.map(_.name).find(name => members.forall(_.protocols.exists(_.name == name)))
    }

  private def expireMembers(): Unit = synchronized {
    val now = System.nanoTime()
    groups.valuesIterator.foreach { group =>
      removeExpiredPendingIds(group, now)
      if group.phase == GroupPhase.Stable then
        val expired = group.members.valuesIterator
          .filter(member => now - member.lastHeartbeatNanos >= member.sessionTimeoutMillis.toLong * 1_000_000L)
          .map(_.memberId)
          .toVector
        if expired.nonEmpty then
          expired.foreach(group.members.remove)
          if group.members.isEmpty then resetEmpty(group) else beginRebalance(group, now)
          notifyAll()
    }
  }

  private def removeExpiredPendingIds(group: ManagedGroup, now: Long): Unit =
    group.pendingMemberIds.iterator.filter(_._2 <= now).map(_._1).toVector.foreach(group.pendingMemberIds.remove)

  private def resetEmpty(group: ManagedGroup): Unit =
    group.phase = GroupPhase.Empty
    group.joined.clear()
    group.leaderId = ""
    group.protocolName = ""

  private def newMemberId(clientId: String): String =
    val prefix = if clientId.isEmpty then "consumer" else clientId
    s"$prefix-${UUID.randomUUID()}"

package cascade.storage

import cascade.protocol.ProtocolException

/** A deterministic lifecycle decision that every replica can apply independently. */
final case class PartitionCleanupPlan(
    deleteBeforeOffset: Long,
    compactBeforeOffset: Long,
    tombstoneCutoffMillis: Long
):
  require(deleteBeforeOffset >= 0L, "cleanup delete offset must be non-negative")
  require(compactBeforeOffset >= deleteBeforeOffset, "compaction boundary cannot precede deletion")

  def hasWork(logStartOffset: Long): Boolean =
    deleteBeforeOffset > logStartOffset || compactBeforeOffset > logStartOffset

/** A leader-epoch-fenced cleanup plan. Generation is monotonic within one leader epoch. */
final case class PartitionCleanupDecision(
    leaderEpoch: Int,
    generation: Long,
    plan: PartitionCleanupPlan
):
  require(leaderEpoch >= 0, "cleanup leader epoch must be non-negative")
  require(generation > 0L, "cleanup generation must be positive")

  def supersedes(other: PartitionCleanupDecision): Boolean =
    leaderEpoch > other.leaderEpoch || leaderEpoch == other.leaderEpoch && generation > other.generation

private[storage] final case class PartitionCleanupState(
    prepared: Option[PartitionCleanupDecision],
    committed: Option[PartitionCleanupDecision],
    applied: Option[PartitionCleanupDecision]
):
  validate()

  def nextGeneration(leaderEpoch: Int): Long =
    val inEpoch = Vector(prepared, committed, applied).flatten
      .filter(_.leaderEpoch == leaderEpoch)
      .map(_.generation)
      .maxOption
      .getOrElse(0L)
    Math.addExact(inEpoch, 1L)

  def prepare(decision: PartitionCleanupDecision): PartitionCleanupState =
    val latest = Vector(prepared, committed, applied).flatten.maxByOption(value => (value.leaderEpoch, value.generation))
    latest match
      case Some(value) if value == decision => copy(prepared = Some(decision))
      case Some(value) if !decision.supersedes(value) =>
        throw ProtocolException(
          s"stale cleanup decision epoch=${decision.leaderEpoch}, generation=${decision.generation}; " +
            s"latest epoch=${value.leaderEpoch}, generation=${value.generation}"
        )
      case _ => copy(prepared = Some(decision))

  def commit(decision: PartitionCleanupDecision): PartitionCleanupState =
    if committed.contains(decision) then this
    else if !prepared.contains(decision) then
      throw ProtocolException("cleanup commit does not match the prepared decision")
    else copy(committed = Some(decision))

  def markApplied(decision: PartitionCleanupDecision): PartitionCleanupState =
    if applied.contains(decision) then this
    else if !committed.contains(decision) then
      throw ProtocolException("cleanup apply does not match the committed decision")
    else copy(applied = Some(decision))

  private def validate(): Unit =
    if committed.exists(value => prepared.exists(preparedValue => !notAfter(value, preparedValue))) then
      throw ProtocolException("cleanup checkpoint committed decision is newer than its prepared decision")
    if applied.exists(value => committed.forall(committedValue => !notAfter(value, committedValue))) then
      throw ProtocolException("cleanup checkpoint applied decision is newer than its committed decision")

  private def notAfter(left: PartitionCleanupDecision, right: PartitionCleanupDecision): Boolean =
    left == right || right.supersedes(left)

private[storage] object PartitionCleanupState:
  val Empty: PartitionCleanupState = PartitionCleanupState(None, None, None)

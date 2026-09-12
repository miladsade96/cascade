package cascade.coordinator

import java.util.UUID

final case class CoordinatorTransactionId(high: Long, low: Long):
  def uuid: UUID = UUID(high, low)

object CoordinatorTransactionId:
  val Checkpoint: CoordinatorTransactionId = CoordinatorTransactionId(0L, 0L)

  def random(): CoordinatorTransactionId =
    val value = UUID.randomUUID()
    CoordinatorTransactionId(value.getMostSignificantBits, value.getLeastSignificantBits)

enum CoordinatorQuorumPhase(val id: Byte):
  case Prepare extends CoordinatorQuorumPhase(0)
  case Decide extends CoordinatorQuorumPhase(1)
  case Finalize extends CoordinatorQuorumPhase(2)
  case Abort extends CoordinatorQuorumPhase(3)
  case Commit extends CoordinatorQuorumPhase(4)
  case Recover extends CoordinatorQuorumPhase(5)
  case Checkpoint extends CoordinatorQuorumPhase(6)

object CoordinatorQuorumPhase:
  def fromId(id: Byte): CoordinatorQuorumPhase = values.find(_.id == id)
    .getOrElse(throw IllegalArgumentException(s"unknown coordinator quorum phase: $id"))

final case class CoordinatorShardCheckpoint(
    shard: Int,
    shardVersion: Long,
    imageVersion: Long,
    ownerTerm: Long,
    payload: Vector[Byte]
):
  require(CoordinatorShard.valid(shard), "invalid checkpoint shard ID")
  require(shardVersion >= 0L && imageVersion >= 0L && ownerTerm >= 0L, "negative coordinator checkpoint version")

final case class CoordinatorQuorumRecord(
    transactionId: CoordinatorTransactionId,
    phase: CoordinatorQuorumPhase,
    delta: Option[CoordinatorDelta],
    certificate: Option[CoordinatorDecisionCertificate] = None,
    checkpoint: Option[CoordinatorShardCheckpoint] = None
):
  require(
    Set(CoordinatorQuorumPhase.Prepare, CoordinatorQuorumPhase.Recover).contains(phase) == delta.nonEmpty,
    "only prepare and recovery records carry a coordinator delta"
  )
  require(
    Set(CoordinatorQuorumPhase.Commit, CoordinatorQuorumPhase.Recover).contains(phase) == certificate.nonEmpty,
    "only commit and recovery records carry a decision certificate"
  )
  require((phase == CoordinatorQuorumPhase.Checkpoint) == checkpoint.nonEmpty, "only checkpoint records carry a shard image")

  def shards: Vector[Int] = (delta.toVector.flatMap(_.updates.map(_.id)) ++ checkpoint.map(_.shard)).distinct.sorted

object CoordinatorQuorumRecord:
  def prepare(transactionId: CoordinatorTransactionId, delta: CoordinatorDelta): CoordinatorQuorumRecord =
    CoordinatorQuorumRecord(transactionId, CoordinatorQuorumPhase.Prepare, Some(delta))

  def marker(transactionId: CoordinatorTransactionId, phase: CoordinatorQuorumPhase): CoordinatorQuorumRecord =
    require(
      Set(CoordinatorQuorumPhase.Decide, CoordinatorQuorumPhase.Finalize, CoordinatorQuorumPhase.Abort).contains(phase),
      "the selected phase requires additional coordinator data"
    )
    CoordinatorQuorumRecord(transactionId, phase, None)

  def commit(
      transactionId: CoordinatorTransactionId,
      certificate: CoordinatorDecisionCertificate
  ): CoordinatorQuorumRecord =
    CoordinatorQuorumRecord(transactionId, CoordinatorQuorumPhase.Commit, None, Some(certificate))

  def recover(
      transactionId: CoordinatorTransactionId,
      delta: CoordinatorDelta,
      certificate: CoordinatorDecisionCertificate
  ): CoordinatorQuorumRecord =
    CoordinatorQuorumRecord(transactionId, CoordinatorQuorumPhase.Recover, Some(delta), Some(certificate))

  def checkpoint(value: CoordinatorShardCheckpoint): CoordinatorQuorumRecord =
    CoordinatorQuorumRecord(CoordinatorTransactionId.Checkpoint, CoordinatorQuorumPhase.Checkpoint, None, None, Some(value))

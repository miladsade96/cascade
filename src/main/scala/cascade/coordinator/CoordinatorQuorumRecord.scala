package cascade.coordinator

import java.util.UUID

final case class CoordinatorTransactionId(high: Long, low: Long):
  def uuid: UUID = UUID(high, low)

object CoordinatorTransactionId:
  def random(): CoordinatorTransactionId =
    val value = UUID.randomUUID()
    CoordinatorTransactionId(value.getMostSignificantBits, value.getLeastSignificantBits)

enum CoordinatorQuorumPhase(val id: Byte):
  case Prepare extends CoordinatorQuorumPhase(0)
  case Decide extends CoordinatorQuorumPhase(1)
  case Finalize extends CoordinatorQuorumPhase(2)
  case Abort extends CoordinatorQuorumPhase(3)

object CoordinatorQuorumPhase:
  def fromId(id: Byte): CoordinatorQuorumPhase = values.find(_.id == id)
    .getOrElse(throw IllegalArgumentException(s"unknown coordinator quorum phase: $id"))

final case class CoordinatorQuorumRecord(
    transactionId: CoordinatorTransactionId,
    phase: CoordinatorQuorumPhase,
    delta: Option[CoordinatorDelta]
):
  require(
    (phase == CoordinatorQuorumPhase.Prepare) == delta.nonEmpty,
    "only prepare records carry a coordinator delta"
  )

  def shards: Vector[Int] = delta.toVector.flatMap(_.updates.map(_.id)).distinct.sorted

object CoordinatorQuorumRecord:
  def prepare(transactionId: CoordinatorTransactionId, delta: CoordinatorDelta): CoordinatorQuorumRecord =
    CoordinatorQuorumRecord(transactionId, CoordinatorQuorumPhase.Prepare, Some(delta))

  def marker(transactionId: CoordinatorTransactionId, phase: CoordinatorQuorumPhase): CoordinatorQuorumRecord =
    require(phase != CoordinatorQuorumPhase.Prepare, "prepare records require a delta")
    CoordinatorQuorumRecord(transactionId, phase, None)

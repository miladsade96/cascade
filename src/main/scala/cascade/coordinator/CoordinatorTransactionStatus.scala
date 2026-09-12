package cascade.coordinator

/** Durable participant state ordered by how far a coordinator transaction advanced. */
enum CoordinatorTransactionStatus(val id: Byte):
  case Unknown extends CoordinatorTransactionStatus(0)
  case Prepared extends CoordinatorTransactionStatus(1)
  case Voted extends CoordinatorTransactionStatus(2)
  case Committed extends CoordinatorTransactionStatus(3)
  case Finalized extends CoordinatorTransactionStatus(4)
  case Aborted extends CoordinatorTransactionStatus(5)

object CoordinatorTransactionStatus:
  def fromId(id: Byte): CoordinatorTransactionStatus =
    values.find(_.id == id).getOrElse(throw IllegalArgumentException(s"unknown coordinator transaction status: $id"))

final case class CoordinatorRecoveryCandidate(
    transactionId: CoordinatorTransactionId,
    status: CoordinatorTransactionStatus,
    delta: CoordinatorDelta,
    certificate: Option[CoordinatorDecisionCertificate],
    observedAtMillis: Long
):
  require(status != CoordinatorTransactionStatus.Unknown, "a recovery candidate must be durable")
  require(delta.updates.nonEmpty, "a recovery candidate must touch a shard")
  require(
    (status == CoordinatorTransactionStatus.Committed) == certificate.nonEmpty,
    "only committed candidates carry a certificate"
  )

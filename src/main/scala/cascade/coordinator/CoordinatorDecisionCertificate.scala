package cascade.coordinator

import cascade.cluster.{QuorumMembership, VoterDirectoryId}

final case class CoordinatorCertificateVoter(nodeId: Int, directoryId: VoterDirectoryId):
  require(nodeId >= 0, "certificate voter ID must be non-negative")
  require(!directoryId.isZero, "certificate voter directory ID must not be zero")

/** Self-contained proof that both sides of the decision-time membership accepted a decision vote. */
final case class CoordinatorDecisionCertificate(
    currentVoters: Vector[CoordinatorCertificateVoter],
    nextVoters: Vector[CoordinatorCertificateVoter],
    acknowledgedNodeIds: Vector[Int]
):
  require(currentVoters.nonEmpty, "a decision certificate requires a voter set")
  require(unique(currentVoters), "current certificate voters must be unique")
  require(unique(nextVoters), "next certificate voters must be unique")
  require(acknowledgedNodeIds.distinct == acknowledgedNodeIds.sorted, "acknowledged voters must be sorted and unique")
  require(valid, "decision certificate does not contain the required majority")

  def valid: Boolean =
    majority(currentVoters) && (nextVoters.isEmpty || majority(nextVoters))

  private def majority(voters: Vector[CoordinatorCertificateVoter]): Boolean =
    voters.count(voter => acknowledgedNodeIds.contains(voter.nodeId)) >= voters.size / 2 + 1

  private def unique(voters: Vector[CoordinatorCertificateVoter]): Boolean =
    voters.map(_.nodeId).distinct.size == voters.size && voters.map(_.directoryId).distinct.size == voters.size

object CoordinatorDecisionCertificate:
  def from(membership: QuorumMembership, acknowledgedNodeIds: Set[Int]): CoordinatorDecisionCertificate =
    def voters(values: Vector[cascade.cluster.QuorumVoter]): Vector[CoordinatorCertificateVoter] =
      values.sortBy(_.id).map(voter => CoordinatorCertificateVoter(voter.id, voter.directoryId))
    CoordinatorDecisionCertificate(
      voters(membership.currentVoters),
      voters(membership.nextVoters),
      acknowledgedNodeIds.toVector.sorted
    )

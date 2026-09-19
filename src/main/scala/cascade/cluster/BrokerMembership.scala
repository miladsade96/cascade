package cascade.cluster

/** Read-only membership projection shared by election, routing, and recovery code. */
final class BrokerMembership(bootstrapNodes: Vector[ClusterNode], localNode: ClusterNode):
  private val bootstrap =
    Option.when(bootstrapNodes.nonEmpty)(QuorumMembership.bootstrap(bootstrapNodes.sortBy(_.id)))
  private val bootstrapById = bootstrapNodes.map(node => node.id -> node).toMap

  def effective(metadata: ClusterMetadata): QuorumMembership =
    metadata.membership.orElse(bootstrap).getOrElse(QuorumMembership.bootstrap(Vector(localNode)))

  def nodes(metadata: ClusterMetadata): Vector[ClusterNode] =
    effective(metadata).currentVoters.map(_.node).sortBy(_.id)

  def activeNodeIds(metadata: ClusterMetadata): Set[Int] =
    effective(metadata).currentVoters.iterator.map(_.id).toSet

  def knownNode(metadata: ClusterMetadata, nodeId: Int): Option[ClusterNode] =
    effective(metadata).voters.find(_.id == nodeId).map(_.node).orElse(bootstrapById.get(nodeId))


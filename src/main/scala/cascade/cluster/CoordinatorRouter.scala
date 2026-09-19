package cascade.cluster

import cascade.coordinator.CoordinatorKey

/** Coordinator ownership policy independent of mutable controller orchestration. */
object CoordinatorRouter:
  def owner(
      key: CoordinatorKey,
      clusterEnabled: Boolean,
      localNode: ClusterNode,
      controller: Option[ClusterNode],
      membership: QuorumMembership,
      unavailableBrokerIds: Set[Int],
      shardingEnabled: Boolean,
      failoverEnabled: Boolean
  ): Option[ClusterNode] =
    if !clusterEnabled then Some(localNode)
    else if !shardingEnabled || !failoverEnabled then controller
    else
      val available = membership.currentVoters.iterator.map(_.node)
        .filterNot(node => unavailableBrokerIds.contains(node.id)).toVector
      CoordinatorRouting.owner(key.routingKey, available).orElse(controller)


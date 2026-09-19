package cascade.cluster

/** Pure partition-leadership transitions used by failure and reassignment paths. */
object PartitionLeadership:
  def removeFailedReplica(partition: PartitionMetadata, failedNodeId: Int): PartitionMetadata =
    if !partition.inSyncReplicas.contains(failedNodeId) then partition
    else
      val remaining = partition.inSyncReplicas.filterNot(_ == failedNodeId)
      val leader = if partition.leaderId == failedNodeId then remaining.headOption.getOrElse(-1) else partition.leaderId
      partition.copy(
        leaderId = leader,
        leaderEpoch = Math.addExact(partition.leaderEpoch, 1),
        inSyncReplicas = remaining
      )

  def finalizeReassignment(partition: PartitionMetadata): PartitionMetadata =
    val target = partition.targetReplicas
    if partition.isReassigning && target.nonEmpty && target.forall(partition.inSyncReplicas.contains) then
      val inSync = target.filter(partition.inSyncReplicas.contains)
      val leader = if target.contains(partition.leaderId) then partition.leaderId else inSync.head
      partition.copy(
        leaderId = leader,
        leaderEpoch = Math.addExact(partition.leaderEpoch, 1),
        replicas = target,
        inSyncReplicas = inSync,
        addingReplicas = Vector.empty,
        removingReplicas = Vector.empty
      )
    else partition


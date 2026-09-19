package cascade.cluster

/** Deterministic election timing and quorum-lease policy, separated from network orchestration. */
final class ControllerElection(
    nodeId: Int,
    preferredControllerId: Int,
    heartbeatMillis: Int,
    electionTimeoutMillis: Int
):
  require(heartbeatMillis > 0, "controller heartbeat must be positive")
  require(electionTimeoutMillis > heartbeatMillis, "controller election timeout must exceed heartbeat interval")

  def deadlineNanos(
      nowNanos: Long,
      initial: Boolean,
      term: Long,
      membership: QuorumMembership
  ): Long =
    val base = electionTimeoutMillis.toLong
    val delayMillis =
      if initial && nodeId == preferredControllerId then heartbeatMillis.toLong * 2L
      else if initial then
        val rank = membership.voters.indexWhere(_.id == nodeId).max(0)
        base + rank.toLong * heartbeatMillis.toLong
      else
        val jitterRange = math.max(1L, base / 2L)
        val jitter = Math.floorMod(nodeId.toLong * 1_103_515_245L + term * 12_345L, jitterRange)
        base + jitter
    nowNanos + Math.multiplyExact(delayMillis, 1_000_000L)

  def hasLease(lastQuorumContactNanos: Long, nowNanos: Long): Boolean =
    lastQuorumContactNanos != 0L &&
      nowNanos - lastQuorumContactNanos < electionTimeoutMillis.toLong * 1_000_000L


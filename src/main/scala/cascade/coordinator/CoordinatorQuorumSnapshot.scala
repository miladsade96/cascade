package cascade.coordinator

final case class CoordinatorQuorumSnapshot(
    inflight: Int = 0,
    peakInflight: Int = 0,
    attempts: Long = 0L,
    committed: Long = 0L,
    failed: Long = 0L,
    rejected: Long = 0L,
    prepareMessages: Long = 0L,
    decisionMessages: Long = 0L,
    finalizeMessages: Long = 0L,
    abortMessages: Long = 0L,
    certificateMessages: Long = 0L,
    recoveryMessages: Long = 0L,
    resolverRuns: Long = 0L,
    recoveredTransactions: Long = 0L,
    recoveryAborts: Long = 0L,
    unresolvedTransactions: Long = 0L,
    phaseNanos: Long = 0L,
    recordBytes: Long = 0L,
    store: CoordinatorShardStoreSnapshot = CoordinatorShardStoreSnapshot()
)

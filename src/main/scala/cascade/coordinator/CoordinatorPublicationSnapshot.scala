package cascade.coordinator

/** Controller-scoped, bounded-label publication queue measurements. */
final case class CoordinatorPublicationSnapshot(
    pendingRequests: Int = 0,
    pendingBytes: Long = 0L,
    peakRequests: Int = 0,
    peakBytes: Long = 0L,
    accepted: Long = 0L,
    rejected: Long = 0L,
    completed: Long = 0L,
    failed: Long = 0L,
    batches: Long = 0L,
    batchRequests: Long = 0L,
    committedBatches: Long = 0L,
    committedRequests: Long = 0L,
    conflictedRequests: Long = 0L,
    queueNanos: Long = 0L
)

package cascade.coordinator

/** Controller-local bounds for combining compatible coordinator shard deltas. */
final case class CoordinatorPublicationConfig(
    maxRequests: Int = 64,
    maxBytes: Long = 16L * 1024 * 1024,
    maxPendingRequests: Int = 1024,
    maxPendingBytes: Long = 64L * 1024 * 1024,
    lingerMillis: Long = 2L,
    queueTimeoutMillis: Long = 5000L
):
  require(maxRequests >= 1 && maxRequests <= 1024, "coordinator publication requests must be in 1..1024")
  require(maxBytes >= 1024L && maxBytes <= 64L * 1024 * 1024, "coordinator publication bytes must be in 1 KiB..64 MiB")
  require(maxPendingRequests >= maxRequests && maxPendingRequests <= 65536, "invalid pending coordinator publication request limit")
  require(maxPendingBytes >= maxBytes && maxPendingBytes <= 512L * 1024 * 1024, "invalid pending coordinator publication byte limit")
  require(lingerMillis >= 0L && lingerMillis <= 100L, "coordinator publication linger must be in 0..100 ms")
  require(queueTimeoutMillis > lingerMillis && queueTimeoutMillis <= 60000L, "invalid coordinator publication queue timeout")

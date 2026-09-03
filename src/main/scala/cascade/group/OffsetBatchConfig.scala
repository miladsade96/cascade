package cascade.group

/** Broker-local admission bounds; they do not change the Kafka protocol or durability policy. */
final case class OffsetBatchConfig(
    maxRequests: Int = 64,
    maxBytes: Long = 1024L * 1024,
    maxPendingRequests: Int = 1024,
    maxPendingBytes: Long = 16L * 1024 * 1024,
    lingerMillis: Long = 2L,
    queueTimeoutMillis: Long = 5000L
):
  require(maxRequests >= 1 && maxRequests <= 1024, "offset batch requests must be in 1..1024")
  require(maxBytes >= 1024L && maxBytes <= 16L * 1024 * 1024, "offset batch bytes must be in 1 KiB..16 MiB")
  require(maxPendingRequests >= maxRequests && maxPendingRequests <= 65536, "invalid pending offset request limit")
  require(maxPendingBytes >= maxBytes && maxPendingBytes <= 256L * 1024 * 1024, "invalid pending offset byte limit")
  require(lingerMillis >= 0L && lingerMillis <= 100L, "offset batch linger must be in 0..100 ms")
  require(queueTimeoutMillis > lingerMillis && queueTimeoutMillis <= 60000L, "invalid offset queue timeout")

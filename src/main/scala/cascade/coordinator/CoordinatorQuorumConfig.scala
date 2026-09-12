package cascade.coordinator

final case class CoordinatorQuorumConfig(
    maxInflightTransactions: Int = 256,
    admissionTimeoutMillis: Long = 5000L,
    resolutionIntervalMillis: Long = 1000L,
    resolutionDelayMillis: Long = 10000L,
    journalCompactionBytes: Long = 64L * 1024L * 1024L
):
  require(maxInflightTransactions >= 1 && maxInflightTransactions <= 65536, "invalid coordinator quorum in-flight limit")
  require(admissionTimeoutMillis >= 1L && admissionTimeoutMillis <= 60000L, "invalid coordinator quorum admission timeout")
  require(resolutionIntervalMillis >= 100L && resolutionIntervalMillis <= 60000L, "invalid coordinator resolution interval")
  require(resolutionDelayMillis >= resolutionIntervalMillis && resolutionDelayMillis <= 300000L, "invalid coordinator resolution delay")
  require(journalCompactionBytes >= 1024L, "coordinator shard journal compaction threshold must be at least 1 KiB")

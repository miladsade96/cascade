package cascade.coordinator

final case class CoordinatorQuorumConfig(
    maxInflightTransactions: Int = 256,
    admissionTimeoutMillis: Long = 5000L
):
  require(maxInflightTransactions >= 1 && maxInflightTransactions <= 65536, "invalid coordinator quorum in-flight limit")
  require(admissionTimeoutMillis >= 1L && admissionTimeoutMillis <= 60000L, "invalid coordinator quorum admission timeout")

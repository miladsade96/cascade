package cascade.storage

enum CleanupPolicy(val deleteEnabled: Boolean, val compactEnabled: Boolean):
  case Delete extends CleanupPolicy(true, false)
  case Compact extends CleanupPolicy(false, true)
  case CompactDelete extends CleanupPolicy(true, true)

object CleanupPolicy:
  def parse(value: String): CleanupPolicy =
    value.split(',').iterator.map(_.trim.toLowerCase).filter(_.nonEmpty).toSet match
      case policies if policies == Set("delete") => CleanupPolicy.Delete
      case policies if policies == Set("compact") => CleanupPolicy.Compact
      case policies if policies == Set("delete", "compact") => CleanupPolicy.CompactDelete
      case _ => throw IllegalArgumentException(s"unsupported cleanup policy: $value")

final case class StorageLifecycleConfig(
    cleanupPolicy: CleanupPolicy = CleanupPolicy.Delete,
    retentionMillis: Long = 7L * 24 * 60 * 60 * 1000,
    retentionBytes: Long = -1L,
    lifecycleIntervalMillis: Long = 5L * 60 * 1000,
    minimumFreeBytes: Long = 0L,
    offsetRetentionMillis: Long = 7L * 24 * 60 * 60 * 1000,
    journalCompactionBytes: Long = 64L * 1024 * 1024,
    deleteRetentionMillis: Long = 24L * 60 * 60 * 1000,
    compactionMaxBytesPerSecond: Long = -1L
):
  require(retentionMillis == -1L || retentionMillis > 0L, "retention time must be -1 or positive")
  require(retentionBytes == -1L || retentionBytes > 0L, "retention bytes must be -1 or positive")
  require(lifecycleIntervalMillis > 0L, "lifecycle interval must be positive")
  require(minimumFreeBytes >= 0L, "minimum free disk bytes must be non-negative")
  require(offsetRetentionMillis == -1L || offsetRetentionMillis > 0L, "offset retention must be -1 or positive")
  require(journalCompactionBytes >= 1024L, "journal compaction threshold must be at least 1 KiB")
  require(deleteRetentionMillis >= 0L, "tombstone retention cannot be negative")
  require(compactionMaxBytesPerSecond == -1L || compactionMaxBytesPerSecond > 0L, "compaction limit must be -1 or positive")

final case class TopicLifecyclePolicy(
    cleanupPolicy: CleanupPolicy,
    retentionMillis: Long,
    retentionBytes: Long
):
  require(retentionMillis == -1L || retentionMillis > 0L, "topic retention time must be -1 or positive")
  require(retentionBytes == -1L || retentionBytes > 0L, "topic retention bytes must be -1 or positive")

  def applyTo(defaults: StorageLifecycleConfig): StorageLifecycleConfig =
    defaults.copy(cleanupPolicy = cleanupPolicy, retentionMillis = retentionMillis, retentionBytes = retentionBytes)

object TopicLifecyclePolicy:
  def from(config: StorageLifecycleConfig): TopicLifecyclePolicy =
    TopicLifecyclePolicy(config.cleanupPolicy, config.retentionMillis, config.retentionBytes)

final case class LifecycleStatistics(
    runs: Long,
    retiredSegments: Long,
    reclaimedBytes: Long,
    compactedBatches: Long,
    rejectedAppends: Long
):
  def +(other: LifecycleStatistics): LifecycleStatistics =
    LifecycleStatistics(
      runs + other.runs,
      retiredSegments + other.retiredSegments,
      reclaimedBytes + other.reclaimedBytes,
      compactedBatches + other.compactedBatches,
      rejectedAppends + other.rejectedAppends
    )

object LifecycleStatistics:
  val Empty: LifecycleStatistics = LifecycleStatistics(0L, 0L, 0L, 0L, 0L)

final class StoragePressureException(message: String) extends RuntimeException(message)

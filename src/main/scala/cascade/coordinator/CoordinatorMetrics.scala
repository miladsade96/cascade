package cascade.coordinator

import java.util.concurrent.atomic.LongAdder

final case class CoordinatorMetricsSnapshot(
    attempts: Long, failures: Long, deltaBytes: Long, fullImageBytes: Long, changedShards: Long, nanos: Long,
    encodedShards: Long = 0L, reusedShards: Long = 0L, encodedBytes: Long = 0L, preparationNanos: Long = 0L
)
object CoordinatorMetricsSnapshot:
  val Empty: CoordinatorMetricsSnapshot = CoordinatorMetricsSnapshot(0L, 0L, 0L, 0L, 0L, 0L)

final class CoordinatorMetrics:
  private val attempts = LongAdder()
  private val failures = LongAdder()
  private val deltaBytes = LongAdder()
  private val fullImageBytes = LongAdder()
  private val changedShards = LongAdder()
  private val nanos = LongAdder()
  private val encodedShards = LongAdder()
  private val reusedShards = LongAdder()
  private val encodedBytes = LongAdder()
  private val preparationNanos = LongAdder()

  private[cascade] def recordPreparation(candidate: CoordinatorSnapshot, duration: Long): Unit =
    encodedShards.add(candidate.encoded.toLong)
    reusedShards.add(candidate.reused.toLong)
    encodedBytes.add(candidate.encodedBytes)
    preparationNanos.add(math.max(0L, duration))

  def record(success: Boolean, deltaSize: Long, fullSize: Long, shards: Int, duration: Long): Unit =
    attempts.increment()
    if !success then failures.increment()
    deltaBytes.add(deltaSize)
    fullImageBytes.add(fullSize)
    changedShards.add(shards.toLong)
    nanos.add(math.max(0L, duration))

  def snapshot: CoordinatorMetricsSnapshot =
    CoordinatorMetricsSnapshot(attempts.sum(), failures.sum(), deltaBytes.sum(), fullImageBytes.sum(), changedShards.sum(), nanos.sum(),
      encodedShards.sum(), reusedShards.sum(), encodedBytes.sum(), preparationNanos.sum())

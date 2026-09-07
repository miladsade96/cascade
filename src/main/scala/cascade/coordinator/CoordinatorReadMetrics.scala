package cascade.coordinator

import java.util.concurrent.atomic.LongAdder

final case class CoordinatorReadSnapshot(
    offsetSnapshots: Long = 0L,
    offsetKeys: Long = 0L,
    stableOffsetSnapshots: Long = 0L,
    transactionVisibilitySnapshots: Long = 0L
)

/** Bounded-cardinality counters for coordinator reads served from acknowledged views. */
final class CoordinatorReadMetrics:
  private val offsetSnapshots = LongAdder()
  private val offsetKeys = LongAdder()
  private val stableOffsetSnapshots = LongAdder()
  private val transactionVisibilitySnapshots = LongAdder()

  def recordOffsets(keys: Int): Unit =
    offsetSnapshots.increment()
    offsetKeys.add(math.max(0, keys).toLong)

  def recordStableOffset(): Unit = stableOffsetSnapshots.increment()

  def recordTransactionVisibility(): Unit = transactionVisibilitySnapshots.increment()

  def snapshot: CoordinatorReadSnapshot = CoordinatorReadSnapshot(
    offsetSnapshots.sum(),
    offsetKeys.sum(),
    stableOffsetSnapshots.sum(),
    transactionVisibilitySnapshots.sum()
  )

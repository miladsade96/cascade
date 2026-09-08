package cascade.coordinator

import java.util.concurrent.atomic.LongAdder

final case class CoordinatorReadSnapshot(
    offsetSnapshots: Long = 0L,
    offsetKeys: Long = 0L,
    stableOffsetSnapshots: Long = 0L,
    transactionVisibilitySnapshots: Long = 0L,
    groupListSnapshots: Long = 0L,
    groupListEntries: Long = 0L,
    groupDescribeSnapshots: Long = 0L,
    groupDescribeHits: Long = 0L,
    groupDeleteAttempts: Long = 0L,
    groupDeleteFailures: Long = 0L
)

/** Bounded-cardinality counters for coordinator reads served from acknowledged views. */
final class CoordinatorReadMetrics:
  private val offsetSnapshots = LongAdder()
  private val offsetKeys = LongAdder()
  private val stableOffsetSnapshots = LongAdder()
  private val transactionVisibilitySnapshots = LongAdder()
  private val groupListSnapshots = LongAdder()
  private val groupListEntries = LongAdder()
  private val groupDescribeSnapshots = LongAdder()
  private val groupDescribeHits = LongAdder()
  private val groupDeleteAttempts = LongAdder()
  private val groupDeleteFailures = LongAdder()

  def recordOffsets(keys: Int): Unit =
    offsetSnapshots.increment()
    offsetKeys.add(math.max(0, keys).toLong)

  def recordStableOffset(): Unit = stableOffsetSnapshots.increment()

  def recordTransactionVisibility(): Unit = transactionVisibilitySnapshots.increment()

  def recordGroupList(entries: Int): Unit =
    groupListSnapshots.increment()
    groupListEntries.add(math.max(0, entries).toLong)

  def recordGroupDescribe(found: Boolean): Unit =
    groupDescribeSnapshots.increment()
    if found then groupDescribeHits.increment()

  def recordGroupDelete(succeeded: Boolean): Unit =
    groupDeleteAttempts.increment()
    if !succeeded then groupDeleteFailures.increment()

  def snapshot: CoordinatorReadSnapshot = CoordinatorReadSnapshot(
    offsetSnapshots.sum(),
    offsetKeys.sum(),
    stableOffsetSnapshots.sum(),
    transactionVisibilitySnapshots.sum(),
    groupListSnapshots.sum(),
    groupListEntries.sum(),
    groupDescribeSnapshots.sum(),
    groupDescribeHits.sum(),
    groupDeleteAttempts.sum(),
    groupDeleteFailures.sum()
  )

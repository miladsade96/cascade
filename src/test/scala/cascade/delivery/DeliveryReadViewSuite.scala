package cascade.delivery

import cascade.storage.{RecordBatchMetadata, TopicPartition}
import munit.FunSuite

final class DeliveryReadViewSuite extends FunSuite:
  private val partition = TopicPartition("events", 0)

  test("indexes the earliest active range for each partition") {
    val view = DeliveryReadView.from(image(
      active = Vector(active("first", 8L, 19L), active("second", 3L, 6L)),
      completed = Vector.empty
    ))

    assertEquals(view.lastStableOffset(partition, 25L), 3L)
    assertEquals(view.lastStableOffset(TopicPartition("events", 1), 25L), 25L)
  }

  test("uses the newest completed outcome that covers a transactional batch") {
    val aborted = completed(committed = false, first = 0L, last = 4L)
    val committed = completed(committed = true, first = 5L, last = 9L)
    val view = DeliveryReadView.from(image(Vector.empty, Vector(aborted, committed)))

    assert(view.visible(partition, batch(6L, 7L, transactional = true)))
    assert(!view.visible(partition, batch(1L, 2L, transactional = true)))
    assert(view.visible(partition, batch(1L, 2L, transactional = false)))
    assert(!view.visible(partition, batch(10L, 11L, transactional = true)))
  }

  test("withholds outcomes whose transactional offsets are not acknowledged") {
    val pending = completed(committed = true, first = 0L, last = 4L).copy(offsetsApplied = false)
    val view = DeliveryReadView.from(image(Vector.empty, Vector(pending)))

    assert(!view.visible(partition, batch(0L, 4L, transactional = true)))
  }

  test("a captured view remains immutable after a newer image is indexed") {
    val before = DeliveryReadView.from(image(Vector(active("open", 0L, 4L)), Vector.empty))
    val after = DeliveryReadView.from(image(
      Vector.empty,
      Vector(completed(committed = true, first = 0L, last = 4L))
    ).copy(version = 8L))
    val transactional = batch(0L, 4L, transactional = true)

    assertEquals(before.lastStableOffset(partition, 5L), 0L)
    assert(!before.visible(partition, transactional))
    assertEquals(after.lastStableOffset(partition, 5L), 5L)
    assert(after.visible(partition, transactional))
    assertEquals(before.lastStableOffset(partition, 5L), 0L)
  }

  test("overlapping outcomes preserve newest-first visibility semantics") {
    val older = completed(committed = true, first = 0L, last = 4L)
    val newer = completed(committed = false, first = 0L, last = 4L)
    val view = DeliveryReadView.from(image(Vector.empty, Vector(older, newer)))

    assert(!view.visible(partition, batch(0L, 4L, transactional = true)))
  }

  test("completed outcomes are isolated by producer epoch") {
    val nextEpoch = completed(committed = true, first = 0L, last = 4L).copy(producerEpoch = 2)
    val view = DeliveryReadView.from(image(Vector.empty, Vector(nextEpoch)))
    val oldEpochBatch = batch(0L, 4L, transactional = true)
    val nextEpochBatch = oldEpochBatch.copy(producerEpoch = 2)

    assert(!view.visible(partition, oldEpochBatch))
    assert(view.visible(partition, nextEpochBatch))
  }

  private def image(active: Vector[ActiveTransaction], completed: Vector[CompletedTransaction]): DeliveryImage =
    DeliveryImage.Empty.copy(version = 7L, activeTransactions = active, completedTransactions = completed)

  private def active(id: String, first: Long, last: Long): ActiveTransaction =
    ActiveTransaction(id, 2L, 1, 30_000, 1L, Vector(partition), Vector(TransactionRange("events", 0, first, last)), Vector.empty, Vector.empty)

  private def completed(committed: Boolean, first: Long, last: Long): CompletedTransaction =
    CompletedTransaction("transaction", 2L, 1, committed, offsetsApplied = true, Vector(TransactionRange("events", 0, first, last)), Vector.empty)

  private def batch(first: Long, last: Long, transactional: Boolean): RecordBatchMetadata =
    RecordBatchMetadata(first, last, 2L, 1, 0, 0, 1, transactional, control = false, 0L, 0)

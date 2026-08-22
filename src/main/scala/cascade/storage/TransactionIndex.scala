package cascade.storage

import scala.collection.mutable.ArrayBuffer

final case class TransactionIndexEntry(
    firstOffset: Long,
    lastOffset: Long,
    producerId: Long,
    control: Boolean
)

/** Batch ranges needed for transaction visibility and lifecycle safety checks. */
final class TransactionIndex:
  private val values = ArrayBuffer.empty[TransactionIndexEntry]

  def append(metadata: RecordBatchMetadata): Unit =
    if metadata.transactional || metadata.control then
      if values.lastOption.forall(_.lastOffset < metadata.baseOffset) then
        values += TransactionIndexEntry(
          metadata.baseOffset,
          metadata.lastOffset,
          metadata.producerId,
          metadata.control
        )
      else throw IllegalArgumentException(s"transaction index overlaps at ${metadata.baseOffset}")

  def overlapping(firstOffset: Long, lastOffsetExclusive: Long): Vector[TransactionIndexEntry] =
    values.iterator
      .filter(entry => entry.lastOffset >= firstOffset && entry.firstOffset < lastOffsetExclusive)
      .toVector

  def entries: Vector[TransactionIndexEntry] = values.toVector

  def clear(): Unit = values.clear()

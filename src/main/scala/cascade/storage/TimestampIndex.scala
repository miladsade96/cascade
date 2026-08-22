package cascade.storage

import scala.collection.mutable.ArrayBuffer

final case class TimestampIndexEntry(maxTimestamp: Long, baseOffset: Long)

/** Sparse batch-level timestamp index; the first qualifying batch is the Kafka ListOffsets boundary. */
final class TimestampIndex:
  private val values = ArrayBuffer.empty[TimestampIndexEntry]

  def append(metadata: RecordBatchMetadata): Unit =
    if values.lastOption.forall(_.baseOffset < metadata.baseOffset) then
      values += TimestampIndexEntry(metadata.maxTimestamp, metadata.baseOffset)
    else throw IllegalArgumentException(s"timestamp index offset moved backwards at ${metadata.baseOffset}")

  def offsetFor(timestamp: Long): Option[Long] =
    values.iterator.find(_.maxTimestamp >= timestamp).map(_.baseOffset)

  def entries: Vector[TimestampIndexEntry] = values.toVector

  def clear(): Unit = values.clear()

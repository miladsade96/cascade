package cascade.delivery

import cascade.storage.{RecordBatchMetadata, TopicPartition}

/** Indexed immutable transaction visibility derived only from acknowledged state. */
private[cascade] final case class DeliveryReadView private (
    imageVersion: Long,
    activeFirstOffsets: Map[TopicPartition, Long],
    completedByProducer: Map[(Long, Short), Vector[CompletedTransaction]]
):
  def lastStableOffset(topicPartition: TopicPartition, highWatermark: Long): Long =
    activeFirstOffsets.get(topicPartition).fold(highWatermark)(math.min(highWatermark, _))

  def visible(topicPartition: TopicPartition, batch: RecordBatchMetadata): Boolean =
    if !batch.transactional then true
    else
      completedByProducer
        .getOrElse((batch.producerId, batch.producerEpoch), Vector.empty)
        .find(_.ranges.exists(range =>
          range.topic == topicPartition.topic &&
          range.partition == topicPartition.partition &&
          batch.baseOffset >= range.firstOffset &&
          batch.lastOffset <= range.lastOffset
        ))
        .exists(transaction => transaction.committed && transaction.offsetsApplied)

private[cascade] object DeliveryReadView:
  val Empty: DeliveryReadView = from(DeliveryImage.Empty)

  def from(image: DeliveryImage): DeliveryReadView =
    val firstOffsets = image.activeTransactions.iterator
      .flatMap(_.ranges)
      .map(range => TopicPartition(range.topic, range.partition) -> range.firstOffset)
      .toVector
      .groupMapReduce(_._1)(_._2)(math.min)
    val completed = image.completedTransactions.reverseIterator.toVector
      .groupBy(transaction => (transaction.producerId, transaction.producerEpoch))
    DeliveryReadView(image.version, firstOffsets, completed)

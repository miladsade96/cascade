package cascade.delivery

import cascade.cluster.{ReplicatedAppendResult, ReplicatedAppender}
import cascade.coordinator.CoordinatorCheckpoint
import cascade.group.{CommittedOffset, GroupCoordinator, GroupOffsetKey, OffsetCommitValue}
import cascade.protocol.{Errors, ProtocolException}
import cascade.storage.{RecordBatch, RecordBatchMetadata, TopicPartition, TopicRegistry}
import java.nio.file.Path
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import scala.collection.mutable

final case class InitProducerIdResult(errorCode: Short, producerId: Long, producerEpoch: Short)
final case class DeliveryAppendResult(errorCode: Short, baseOffset: Long)
private final case class TransactionReservation(
    transactionalId: String,
    topicPartition: TopicPartition,
    previousRange: Option[TransactionRange],
    firstOffset: Long
)

/** Durable producer fencing, idempotent sequence validation, and transaction visibility. */
final class DeliveryCoordinator(
    statePath: Path,
    registry: TopicRegistry,
    groups: GroupCoordinator,
    stateLock: Object = Object(),
    durableLocal: Boolean = true,
    scheduleExpiration: Boolean = true
) extends AutoCloseable:
  private val MaximumTransactionTimeoutMillis = 15 * 60 * 1000
  private val store = DeliveryStore(statePath)
  private val partitionLocks = ConcurrentHashMap[TopicPartition, Object]()
  private val inFlightTransactionalAppends = mutable.HashMap.empty[String, Int]
  private val closed = AtomicBoolean(false)
  private var checkpoint: CoordinatorCheckpoint = CoordinatorCheckpoint.Local
  private val expirationExecutor: Option[ScheduledExecutorService] = Option.when(scheduleExpiration) {
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-transaction-expirer").factory())
  }
  @volatile private var current = store.image

  recoverActiveRanges()
  replayCommittedOffsets()
  expirationExecutor.foreach(_.scheduleWithFixedDelay(() => expireNow(), 1L, 1L, TimeUnit.SECONDS): Unit)

  def image: DeliveryImage = current

  def snapshotBytes: Array[Byte] = stateLock.synchronized(DeliveryCodec.encode(current))

  def installSnapshot(bytes: Vector[Byte]): Unit = stateLock.synchronized {
    val image = if bytes.isEmpty then DeliveryImage.Empty else DeliveryCodec.decode(bytes.toArray)
    store.install(image)
    current = image
    stateLock.notifyAll()
  }

  def attachCheckpoint(value: CoordinatorCheckpoint): Unit = stateLock.synchronized {
    checkpoint = value
  }

  def initProducerId(transactionalId: Option[String], timeoutMillis: Int): InitProducerIdResult = stateLock.synchronized {
    transactionalId.foreach(awaitTransactionalAppends)
    expireTransactionsLocked(System.currentTimeMillis())
    if transactionalId.exists(_.isEmpty) then
      InitProducerIdResult(Errors.TransactionalIdAuthorizationFailed, -1L, -1)
    else if timeoutMillis <= 0 || timeoutMillis > MaximumTransactionTimeoutMillis then
      InitProducerIdResult(Errors.InvalidTransactionTimeout, -1L, -1)
    else
      transactionalId.flatMap(current.producerByTransactionalId.get) match
        case Some(existing) =>
          val (producerId, epoch) =
            if existing.producerEpoch == Short.MaxValue then (current.nextProducerId, 0.toShort)
            else (existing.producerId, (existing.producerEpoch + 1).toShort)
          val completed = current.activeByTransactionalId.get(existing.transactionalId.get).toVector.map { active =>
            CompletedTransaction(
              active.transactionalId,
              active.producerId,
              active.producerEpoch,
              committed = false,
              offsetsApplied = true,
              active.ranges,
              Vector.empty
            )
          }
          val replacement = ProducerRegistration(producerId, epoch, transactionalId, timeoutMillis)
          val committed = commit(
            current.copy(
              version = current.version + 1L,
              nextProducerId = if producerId == current.nextProducerId then current.nextProducerId + 1L else current.nextProducerId,
              producers = current.producers.filterNot(_.transactionalId == transactionalId) :+ replacement,
              activeTransactions = current.activeTransactions.filterNot(_.transactionalId == existing.transactionalId.get),
              completedTransactions = current.completedTransactions ++ completed
            )
          )
          if committed then InitProducerIdResult(Errors.None, producerId, epoch)
          else InitProducerIdResult(Errors.CoordinatorNotAvailable, -1L, -1)
        case None =>
          val producerId = current.nextProducerId
          val producer = ProducerRegistration(producerId, 0, transactionalId, timeoutMillis)
          val committed = commit(
            current.copy(
              version = current.version + 1L,
              nextProducerId = producerId + 1L,
              producers = current.producers :+ producer
            )
          )
          if committed then InitProducerIdResult(Errors.None, producerId, 0)
          else InitProducerIdResult(Errors.CoordinatorNotAvailable, -1L, -1)
  }

  def addPartitions(
      transactionalId: String,
      producerId: Long,
      producerEpoch: Short,
      partitions: Vector[TopicPartition]
  ): Short = stateLock.synchronized {
    awaitTransactionalAppends(transactionalId)
    expireTransactionsLocked(System.currentTimeMillis())
    validateProducer(transactionalId, producerId, producerEpoch) match
      case error if error != Errors.None => error
      case _ =>
        current.activeByTransactionalId.get(transactionalId) match
          case Some(active) if active.producerId != producerId || active.producerEpoch != producerEpoch =>
            Errors.ProducerFenced
          case existing =>
            val registration = current.producerById(producerId)
            val active = existing.getOrElse(
              ActiveTransaction(
                transactionalId,
                producerId,
                producerEpoch,
                registration.transactionTimeoutMillis,
                System.currentTimeMillis(),
                Vector.empty,
                Vector.empty,
                Vector.empty,
                Vector.empty
              )
            )
            val next = active.copy(partitions = (active.partitions ++ partitions).distinct)
            val committed = commit(
              current.copy(
                version = current.version + 1L,
                activeTransactions = current.activeTransactions.filterNot(_.transactionalId == transactionalId) :+ next
              )
            )
            if committed then Errors.None else Errors.CoordinatorNotAvailable
  }

  def addOffsets(transactionalId: String, producerId: Long, producerEpoch: Short, groupId: String): Short = stateLock.synchronized {
    awaitTransactionalAppends(transactionalId)
    expireTransactionsLocked(System.currentTimeMillis())
    activeFor(transactionalId, producerId, producerEpoch) match
      case Left(error) => error
      case Right(active) if groupId.isEmpty => Errors.InvalidGroupId
      case Right(active) =>
        val next = active.copy(groups = (active.groups :+ groupId).distinct)
        if replaceActive(next) then Errors.None else Errors.CoordinatorNotAvailable
  }

  def stageOffsets(
      transactionalId: String,
      producerId: Long,
      producerEpoch: Short,
      groupId: String,
      offsets: Vector[PendingOffset]
  ): Short = stateLock.synchronized {
    awaitTransactionalAppends(transactionalId)
    expireTransactionsLocked(System.currentTimeMillis())
    activeFor(transactionalId, producerId, producerEpoch) match
      case Left(error) => error
      case Right(active) if !active.groups.contains(groupId) => Errors.InvalidTxnState
      case Right(active) =>
        val keys = offsets.map(value => (value.groupId, value.topic, value.partition)).toSet
        val next = active.copy(
          pendingOffsets = active.pendingOffsets.filterNot(value => keys((value.groupId, value.topic, value.partition))) ++ offsets
        )
        if replaceActive(next) then Errors.None else Errors.CoordinatorNotAvailable
  }

  def endTransaction(
      transactionalId: String,
      producerId: Long,
      producerEpoch: Short,
      committed: Boolean
  ): Short = stateLock.synchronized {
    awaitTransactionalAppends(transactionalId)
    expireTransactionsLocked(System.currentTimeMillis())
    activeFor(transactionalId, producerId, producerEpoch) match
      case Left(error) => error
      case Right(active) =>
        val useAtomicSnapshot = !durableLocal && committed && active.pendingOffsets.nonEmpty
        if useAtomicSnapshot then groups.stageReplicatedOffsets(offsetValues(active.pendingOffsets))
        val completed = CompletedTransaction(
          transactionalId,
          producerId,
          producerEpoch,
          committed,
          offsetsApplied = !committed || active.pendingOffsets.isEmpty || useAtomicSnapshot,
          active.ranges,
          if committed then active.pendingOffsets else Vector.empty
        )
        val transitionCommitted = commit(
          current.copy(
            version = current.version + 1L,
            activeTransactions = current.activeTransactions.filterNot(_.transactionalId == transactionalId),
            completedTransactions = current.completedTransactions :+ completed
          )
        )
        if !transitionCommitted then return Errors.CoordinatorNotAvailable
        if committed && completed.pendingOffsets.nonEmpty && !useAtomicSnapshot then
          applyOffsets(completed.pendingOffsets)
          if !markOffsetsApplied(completed) then return Errors.CoordinatorNotAvailable
        Errors.None
  }

  def append(
      transactionalId: Option[String],
      topic: String,
      partition: Int,
      records: Array[Byte],
      acknowledgements: Short,
      timeoutMillis: Int,
      replication: ReplicatedAppender
  ): DeliveryAppendResult =
    val topicPartition = TopicPartition(topic, partition)
    val lock = partitionLocks.computeIfAbsent(topicPartition, _ => Object())
    lock.synchronized {
      val log = registry.partition(topic, partition)
      if log.isEmpty then return DeliveryAppendResult(Errors.UnknownTopicOrPartition, -1L)
      val batches = RecordBatch.prepare(records, 0L).map(batch => RecordBatch.metadata(batch.bytes))
      if batches.isEmpty then return DeliveryAppendResult(Errors.InvalidRequest, -1L)
      val producerId = batches.head.producerId
      if batches.exists(batch => batch.producerId != producerId || batch.producerEpoch != batches.head.producerEpoch) then
        return DeliveryAppendResult(Errors.InvalidRequest, -1L)

      if producerId < 0L then
        if transactionalId.nonEmpty || batches.exists(_.transactional) then
          DeliveryAppendResult(Errors.InvalidTxnState, -1L)
        else fromReplication(replication.append(topic, partition, records, acknowledgements, timeoutMillis))
      else
        val span = batches.last.lastOffset - batches.head.baseOffset
        val validation = stateLock.synchronized {
          expireTransactionsLocked(System.currentTimeMillis())
          validateBatchSequence(transactionalId, topicPartition, batches, log.get.recentBatches(producerId)).map { _ =>
            transactionalId.map { id =>
              val firstOffset = log.get.logEndOffset
              val reservation = reserveTransactionalAppend(
                id,
                topicPartition,
                firstOffset,
                Math.addExact(firstOffset, span)
              )
              inFlightTransactionalAppends.update(id, inFlightTransactionalAppends.getOrElse(id, 0) + 1)
              reservation
            }
          }
        }
        validation match
          case Left(result) if result.errorCode == Errors.None && transactionalId.nonEmpty =>
            val committed = stateLock.synchronized {
              recordTransactionalRange(
                transactionalId.get,
                topicPartition,
                result.baseOffset,
                Math.addExact(result.baseOffset, span)
              )
            }
            if committed then result else DeliveryAppendResult(Errors.CoordinatorNotAvailable, -1L)
          case Left(result) => result
          case Right(reservation) =>
            try
              val appended = replication.append(topic, partition, records, acknowledgements, timeoutMillis)
              var coordinatorCommitted = true
              reservation.foreach { reserved =>
                if appended.baseOffset < 0L then rollbackTransactionalAppend(reserved)
                else if appended.baseOffset != reserved.firstOffset then
                  throw ProtocolException(
                    s"transactional append offset changed after reservation: reserved=${reserved.firstOffset}, appended=${appended.baseOffset}"
                  )
                else coordinatorCommitted = commitTransactionalAppend()
              }
              if coordinatorCommitted then fromReplication(appended)
              else DeliveryAppendResult(Errors.CoordinatorNotAvailable, -1L)
            finally reservation.foreach(value => finishTransactionalAppend(value.transactionalId))
    }

  def lastStableOffset(topic: String, partition: Int, highWatermark: Long): Long =
    expireNow()
    current.activeTransactions.iterator
      .flatMap(_.ranges.iterator)
      .filter(range => range.topic == topic && range.partition == partition)
      .map(_.firstOffset)
      .minOption
      .fold(highWatermark)(math.min(highWatermark, _))

  def visible(topic: String, partition: Int, batch: RecordBatchMetadata): Boolean =
    if !batch.transactional then true
    else
      current.completedTransactions.reverseIterator
        .find { transaction =>
          transaction.producerId == batch.producerId &&
          transaction.producerEpoch == batch.producerEpoch &&
          transaction.ranges.exists(range =>
            range.topic == topic && range.partition == partition &&
            batch.baseOffset >= range.firstOffset && batch.lastOffset <= range.lastOffset
          )
        }
        .exists(transaction => transaction.committed && transaction.offsetsApplied)

  def latestOffset(topic: String, partition: Int, highWatermark: Long, readCommitted: Boolean): Long =
    if readCommitted then lastStableOffset(topic, partition, highWatermark) else highWatermark

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      expirationExecutor.foreach { executor =>
        executor.shutdownNow(): Unit
        executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
      }
      store.close()

  private def validateBatchSequence(
      transactionalId: Option[String],
      topicPartition: TopicPartition,
      batches: Vector[RecordBatchMetadata],
      existing: Vector[RecordBatchMetadata]
  ): Either[DeliveryAppendResult, Unit] =
    val first = batches.head
    val registration = current.producerById.get(first.producerId)
    if registration.isEmpty then return Left(DeliveryAppendResult(Errors.UnknownProducerId, -1L))
    if registration.get.producerEpoch != first.producerEpoch then
      return Left(DeliveryAppendResult(Errors.InvalidProducerEpoch, -1L))
    if registration.get.transactionalId != transactionalId then
      return Left(DeliveryAppendResult(Errors.InvalidProducerIdMapping, -1L))
    if batches.exists(_.control) then return Left(DeliveryAppendResult(Errors.InvalidRequest, -1L))
    if batches.exists(batch => batch.baseSequence < 0 || batch.recordCount <= 0) then
      return Left(DeliveryAppendResult(Errors.OutOfOrderSequenceNumber, -1L))
    if !sequencesAreContiguous(batches) then
      return Left(DeliveryAppendResult(Errors.OutOfOrderSequenceNumber, -1L))

    val sameEpoch = existing.filter(_.producerEpoch == first.producerEpoch)
    duplicateBaseOffset(batches, sameEpoch) match
      case Some(baseOffset) => return Left(DeliveryAppendResult(Errors.None, baseOffset))
      case None             => ()

    val expected = sameEpoch.lastOption.map(batch => nextSequence(batch.lastSequence)).getOrElse(0)
    if first.baseSequence != expected then
      return Left(DeliveryAppendResult(Errors.OutOfOrderSequenceNumber, -1L))

    transactionalId match
      case Some(id) =>
        activeFor(id, first.producerId, first.producerEpoch) match
          case Left(error) => Left(DeliveryAppendResult(error, -1L))
          case Right(active) if !active.partitions.contains(topicPartition) =>
            Left(DeliveryAppendResult(Errors.InvalidTxnState, -1L))
          case Right(_) if batches.exists(!_.transactional) =>
            Left(DeliveryAppendResult(Errors.InvalidTxnState, -1L))
          case Right(_) => Right(())
      case None if batches.exists(_.transactional) => Left(DeliveryAppendResult(Errors.InvalidTxnState, -1L))
      case None => Right(())

  private def duplicateBaseOffset(
      batches: Vector[RecordBatchMetadata],
      existing: Vector[RecordBatchMetadata]
  ): Option[Long] =
    existing.indices.iterator.flatMap { start =>
      val candidate = existing.slice(start, start + batches.length)
      Option.when(
        candidate.length == batches.length && candidate.zip(batches).forall { case (stored, incoming) =>
          stored.baseSequence == incoming.baseSequence &&
          stored.lastSequence == incoming.lastSequence &&
          stored.recordCount == incoming.recordCount &&
          stored.transactional == incoming.transactional
        }
      )(candidate.head.baseOffset)
    }.nextOption()

  private def sequencesAreContiguous(batches: Vector[RecordBatchMetadata]): Boolean =
    batches.sliding(2).forall {
      case Vector(previous, next) => next.baseSequence == nextSequence(previous.lastSequence)
      case _                      => true
    }

  private def nextSequence(sequence: Int): Int = if sequence == Int.MaxValue then 0 else sequence + 1

  private def reserveTransactionalAppend(
      transactionalId: String,
      topicPartition: TopicPartition,
      firstOffset: Long,
      lastOffset: Long
  ): TransactionReservation =
    val active = current.activeByTransactionalId(transactionalId)
    val existing = active.ranges.find(range => range.topic == topicPartition.topic && range.partition == topicPartition.partition)
    val updated = existing.fold(TransactionRange(topicPartition.topic, topicPartition.partition, firstOffset, lastOffset)) { range =>
      range.copy(firstOffset = math.min(range.firstOffset, firstOffset), lastOffset = math.max(range.lastOffset, lastOffset))
    }
    replaceActiveInMemory(
      active.copy(
        ranges = active.ranges.filterNot(range =>
          range.topic == topicPartition.topic && range.partition == topicPartition.partition
        ) :+ updated
      )
    )
    TransactionReservation(transactionalId, topicPartition, existing, firstOffset)

  private def rollbackTransactionalAppend(reservation: TransactionReservation): Unit = stateLock.synchronized {
    current.activeByTransactionalId.get(reservation.transactionalId).foreach { active =>
      val withoutReservation = active.ranges.filterNot(range =>
        range.topic == reservation.topicPartition.topic && range.partition == reservation.topicPartition.partition
      )
      val restored = reservation.previousRange.fold(withoutReservation)(withoutReservation :+ _)
      replaceActiveInMemory(active.copy(ranges = restored))
    }
  }

  private def commitTransactionalAppend(): Boolean = stateLock.synchronized {
    current = current.copy(version = Math.addExact(current.version, 1L))
    checkpoint.commit()
  }

  private def recordTransactionalRange(
      transactionalId: String,
      topicPartition: TopicPartition,
      firstOffset: Long,
      lastOffset: Long
  ): Boolean =
    current.activeByTransactionalId.get(transactionalId) match
      case None => false
      case Some(active) =>
        val previous = active.ranges.find(range =>
          range.topic == topicPartition.topic && range.partition == topicPartition.partition
        )
        val updated = previous.fold(TransactionRange(topicPartition.topic, topicPartition.partition, firstOffset, lastOffset)) {
          range => range.copy(firstOffset = math.min(range.firstOffset, firstOffset), lastOffset = math.max(range.lastOffset, lastOffset))
        }
        val next = active.copy(
          ranges = active.ranges.filterNot(range =>
            range.topic == topicPartition.topic && range.partition == topicPartition.partition
          ) :+ updated
        )
        if next == active then true
        else
          replaceActiveInMemory(next)
          commitTransactionalAppend()

  private def finishTransactionalAppend(transactionalId: String): Unit = stateLock.synchronized {
    val remaining = inFlightTransactionalAppends.getOrElse(transactionalId, 0) - 1
    if remaining <= 0 then inFlightTransactionalAppends.remove(transactionalId): Unit
    else inFlightTransactionalAppends.update(transactionalId, remaining)
    stateLock.notifyAll()
  }

  private def awaitTransactionalAppends(transactionalId: String): Unit =
    while inFlightTransactionalAppends.getOrElse(transactionalId, 0) > 0 do stateLock.wait()

  private def validateProducer(transactionalId: String, producerId: Long, producerEpoch: Short): Short =
    current.producerById.get(producerId) match
      case None => Errors.UnknownProducerId
      case Some(producer) if producer.transactionalId != Some(transactionalId) => Errors.InvalidProducerIdMapping
      case Some(producer) if producer.producerEpoch != producerEpoch => Errors.ProducerFenced
      case Some(_) => Errors.None

  private def activeFor(
      transactionalId: String,
      producerId: Long,
      producerEpoch: Short
  ): Either[Short, ActiveTransaction] =
    validateProducer(transactionalId, producerId, producerEpoch) match
      case error if error != Errors.None => Left(error)
      case _ => current.activeByTransactionalId.get(transactionalId).toRight(Errors.InvalidTxnState)

  private def replaceActive(active: ActiveTransaction): Boolean =
    commit(
      current.copy(
        version = current.version + 1L,
        activeTransactions = current.activeTransactions.filterNot(_.transactionalId == active.transactionalId) :+ active
      )
    )

  private def replaceActiveInMemory(active: ActiveTransaction): Unit =
    current = current.copy(
      activeTransactions = current.activeTransactions.filterNot(_.transactionalId == active.transactionalId) :+ active
    )

  private def commit(next: DeliveryImage): Boolean =
    store.commit(next, durableLocal)
    current = next
    checkpoint.commit()

  private def fromReplication(result: ReplicatedAppendResult): DeliveryAppendResult =
    DeliveryAppendResult(result.errorCode, result.baseOffset)

  def expireNow(): Unit = stateLock.synchronized {
    expireTransactionsLocked(System.currentTimeMillis())
  }

  private def expireTransactionsLocked(nowMillis: Long): Unit =
    val expired = current.activeTransactions.filter { active =>
      inFlightTransactionalAppends.getOrElse(active.transactionalId, 0) == 0 &&
      nowMillis - active.startedAtMillis >= active.timeoutMillis.toLong
    }
    if expired.nonEmpty then
      val completed = expired.map { active =>
        CompletedTransaction(
          active.transactionalId,
          active.producerId,
          active.producerEpoch,
          committed = false,
          offsetsApplied = true,
          active.ranges,
          Vector.empty
        )
      }
      val ids = expired.map(_.transactionalId).toSet
      commit(
        current.copy(
          version = current.version + 1L,
          activeTransactions = current.activeTransactions.filterNot(value => ids(value.transactionalId)),
          completedTransactions = current.completedTransactions ++ completed
        )
      ): Unit

  private def applyOffsets(values: Vector[PendingOffset]): Unit =
    offsetValues(values).groupBy(_.key.groupId).foreach { case (groupId, offsets) =>
      groups.commitOffsets(
        groupId,
        -1,
        "",
        offsets
      ): Unit
    }

  private def offsetValues(values: Vector[PendingOffset]): Vector[OffsetCommitValue] =
    val now = System.currentTimeMillis()
    values.map { value =>
      OffsetCommitValue(
        GroupOffsetKey(value.groupId, value.topic, value.partition),
        CommittedOffset(value.offset, value.leaderEpoch, value.metadata, now)
      )
    }

  private def replayCommittedOffsets(): Unit = stateLock.synchronized {
    val pending = current.completedTransactions.filter(transaction => transaction.committed && !transaction.offsetsApplied)
    if pending.nonEmpty then
      pending.foreach(transaction => applyOffsets(transaction.pendingOffsets))
      val pendingSet = pending.toSet
      commit(
        current.copy(
          version = current.version + 1L,
          completedTransactions = current.completedTransactions.map { transaction =>
            if pendingSet(transaction) then transaction.copy(offsetsApplied = true) else transaction
          }
        )
      ): Unit
  }

  private def markOffsetsApplied(completed: CompletedTransaction): Boolean =
    commit(
      current.copy(
        version = current.version + 1L,
        completedTransactions = current.completedTransactions.map { transaction =>
          if transaction == completed then transaction.copy(offsetsApplied = true) else transaction
        }
      )
    )

  private def recoverActiveRanges(): Unit = stateLock.synchronized {
    val covered = current.completedTransactions.flatMap(_.ranges).toSet
    val recovered = current.activeTransactions.map { active =>
      val discovered = active.partitions.flatMap { topicPartition =>
        registry.partition(topicPartition.topic, topicPartition.partition).flatMap { log =>
          val batches = log.producerBatches(active.producerId).filter { batch =>
            batch.producerEpoch == active.producerEpoch && batch.transactional &&
            !covered.exists(range =>
              range.topic == topicPartition.topic && range.partition == topicPartition.partition &&
              batch.baseOffset >= range.firstOffset && batch.lastOffset <= range.lastOffset
            )
          }
          if batches.isEmpty then None
          else Some(
            TransactionRange(
              topicPartition.topic,
              topicPartition.partition,
              batches.map(_.baseOffset).min,
              batches.map(_.lastOffset).max
            )
          )
        }
      }
      active.copy(ranges = mergeRanges(active.ranges ++ discovered))
    }
    if recovered != current.activeTransactions then
      commit(current.copy(version = current.version + 1L, activeTransactions = recovered)): Unit
  }

  private def mergeRanges(values: Vector[TransactionRange]): Vector[TransactionRange] =
    values.groupBy(value => (value.topic, value.partition)).toVector.sortBy(_._1).map { case ((topic, partition), ranges) =>
      TransactionRange(topic, partition, ranges.map(_.firstOffset).min, ranges.map(_.lastOffset).max)
    }

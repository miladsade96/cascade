package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
import cascade.protocol.Errors
import java.nio.file.{Files, Path}
import scala.collection.mutable
import scala.jdk.CollectionConverters.*

final case class CoordinatorShardStoreSnapshot(
    prepared: Long = 0L,
    decided: Long = 0L,
    finalized: Long = 0L,
    aborted: Long = 0L,
    conflicts: Long = 0L,
    pending: Int = 0,
    journalRecords: Long = 0L,
    journalBytes: Long = 0L,
    forceNanos: Long = 0L,
    truncatedBytes: Long = 0L
)

/** Atomic local participant backed by one durable journal per touched shard. */
final class CoordinatorShardStore(directory: Path, baseline: CoordinatorMetadata) extends AutoCloseable:
  private val root = directory.toAbsolutePath.normalize()
  Files.createDirectories(root)
  private val journals = mutable.HashMap.empty[Int, CoordinatorShardJournal]
  private val pending = mutable.HashMap.empty[CoordinatorTransactionId, CoordinatorDelta]
  private val decisions = mutable.HashSet.empty[CoordinatorTransactionId]
  private val preparedParts = mutable.HashMap.empty[CoordinatorTransactionId, mutable.Set[Int]]
  private val decisionParts = mutable.HashMap.empty[CoordinatorTransactionId, mutable.Set[Int]]
  private val finalizedParts = mutable.HashMap.empty[CoordinatorTransactionId, mutable.Set[Int]]
  private var state = baseline
  private var preparedCount = 0L
  private var decidedCount = 0L
  private var finalizedCount = 0L
  private var abortedCount = 0L
  private var conflictCount = 0L
  recover()

  def metadata: CoordinatorMetadata = synchronized(state)

  def prepare(transactionId: CoordinatorTransactionId, delta: CoordinatorDelta, expectedTerm: Long): Short = synchronized {
    pending.get(transactionId) match
      case Some(existing) if existing != delta => Errors.InvalidRequest
      case Some(existing) =>
        appendMissing(existing, CoordinatorQuorumRecord.prepare(transactionId, existing), preparedParts, transactionId)
        Errors.None
      case None if delta.controllerTerm != expectedTerm => Errors.CoordinatorLoadInProgress
      case None if delta.updates.exists(update => state.shardVersion(update.id) != update.expectedVersion) =>
        conflictCount += 1L
        Errors.CoordinatorLoadInProgress
      case None if pending.valuesIterator.exists(other => overlaps(other, delta)) =>
        conflictCount += 1L
        Errors.CoordinatorLoadInProgress
      case None =>
        pending.update(transactionId, delta)
        appendMissing(delta, CoordinatorQuorumRecord.prepare(transactionId, delta), preparedParts, transactionId)
        preparedCount += 1L
        Errors.None
  }

  def decide(transactionId: CoordinatorTransactionId): Short = synchronized {
    pending.get(transactionId) match
      case None => Errors.CoordinatorLoadInProgress
      case Some(delta) =>
        appendMissing(delta, CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Decide), decisionParts, transactionId)
        if complete(transactionId, delta, decisionParts) && !decisions(transactionId) then
          decisions += transactionId
          decidedCount += 1L
        Errors.None
  }

  def finalizeTransaction(transactionId: CoordinatorTransactionId): Either[Short, CoordinatorMetadata] = synchronized {
    pending.get(transactionId) match
      case None => Left(Errors.CoordinatorLoadInProgress)
      case Some(_) if !decisions(transactionId) => Left(Errors.InvalidRequest)
      case Some(delta) =>
        CoordinatorShardState.merge(state, delta, delta.controllerTerm) match
          case Left(_) =>
            conflictCount += 1L
            Left(Errors.CoordinatorLoadInProgress)
          case Right(next) =>
            appendMissing(delta, CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Finalize), finalizedParts, transactionId)
            if !complete(transactionId, delta, finalizedParts) then return Left(Errors.CoordinatorNotAvailable)
            state = next
            pending.remove(transactionId): Unit
            decisions -= transactionId
            preparedParts.remove(transactionId): Unit
            decisionParts.remove(transactionId): Unit
            finalizedParts.remove(transactionId): Unit
            finalizedCount += 1L
            Right(next)
  }

  def abort(transactionId: CoordinatorTransactionId): Unit = synchronized {
    pending.remove(transactionId).foreach { delta =>
      appendAll(delta, CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Abort))
      decisions -= transactionId
      preparedParts.remove(transactionId): Unit
      decisionParts.remove(transactionId): Unit
      finalizedParts.remove(transactionId): Unit
      abortedCount += 1L
    }
  }

  def snapshot: CoordinatorShardStoreSnapshot = synchronized {
    val journal = journals.valuesIterator.map(_.snapshot).toVector
    CoordinatorShardStoreSnapshot(
      preparedCount,
      decidedCount,
      finalizedCount,
      abortedCount,
      conflictCount,
      pending.size,
      journal.map(_.records).sum,
      journal.map(_.bytes).sum,
      journal.map(_.forceNanos).sum,
      journal.map(_.truncatedBytes).sum
    )
  }

  override def close(): Unit = synchronized {
    journals.values.foreach(_.close())
    journals.clear()
  }

  private def appendAll(delta: CoordinatorDelta, record: CoordinatorQuorumRecord): Unit =
    delta.updates.map(_.id).distinct.sorted.foreach(id => journal(id).append(record))

  private def appendMissing(
      delta: CoordinatorDelta,
      record: CoordinatorQuorumRecord,
      parts: mutable.Map[CoordinatorTransactionId, mutable.Set[Int]],
      transactionId: CoordinatorTransactionId
  ): Unit =
    val completed = parts.getOrElseUpdate(transactionId, mutable.HashSet.empty)
    delta.updates.map(_.id).distinct.sorted.filterNot(completed).foreach { id =>
      journal(id).append(record)
      completed += id
    }

  private def complete(
      transactionId: CoordinatorTransactionId,
      delta: CoordinatorDelta,
      parts: mutable.Map[CoordinatorTransactionId, mutable.Set[Int]]
  ): Boolean = delta.updates.forall(update => parts.get(transactionId).exists(_(update.id)))

  private def journal(shard: Int): CoordinatorShardJournal =
    journals.getOrElseUpdate(shard, CoordinatorShardJournal(CoordinatorShardJournal.path(root, shard), shard))

  private def overlaps(left: CoordinatorDelta, right: CoordinatorDelta): Boolean =
    val ids = left.updates.iterator.map(_.id).toSet
    right.updates.exists(update => ids(update.id))

  private def recover(): Unit = synchronized {
    val stream = Files.list(root)
    val paths = try stream.iterator().asScala.filter { path =>
      Files.isRegularFile(path) && path.getFileName.toString.matches("shard-[0-9]{3}\\.log")
    }.toVector.sortBy(_.getFileName.toString)
    finally stream.close()
    paths.foreach { path =>
      val name = path.getFileName.toString
      val shard = name.substring(6, 9).toInt
      if !CoordinatorShard.valid(shard) then throw IllegalStateException(s"invalid coordinator shard journal: $name")
      journals.update(shard, CoordinatorShardJournal(path, shard))
    }

    final case class Recovered(
        var delta: Option[CoordinatorDelta] = None,
        preparedShards: mutable.Set[Int] = mutable.HashSet.empty,
        decidedShards: mutable.Set[Int] = mutable.HashSet.empty,
        finalizedShards: mutable.Set[Int] = mutable.HashSet.empty,
        abortedShards: mutable.Set[Int] = mutable.HashSet.empty
    )
    val transactions = mutable.LinkedHashMap.empty[CoordinatorTransactionId, Recovered]
    journals.toVector.sortBy(_._1).foreach { case (shard, journal) =>
      journal.entries.foreach { record =>
        val recovered = transactions.getOrElseUpdate(record.transactionId, Recovered())
        record.phase match
          case CoordinatorQuorumPhase.Prepare =>
            val delta = record.delta.get
            recovered.delta match
              case Some(existing) if existing != delta => throw IllegalStateException("coordinator transaction payload changed")
              case _ => recovered.delta = Some(delta)
            recovered.preparedShards += shard
          case CoordinatorQuorumPhase.Decide   => recovered.decidedShards += shard
          case CoordinatorQuorumPhase.Finalize => recovered.finalizedShards += shard
          case CoordinatorQuorumPhase.Abort    => recovered.abortedShards += shard
      }
    }
    val remaining = mutable.LinkedHashMap.from(transactions)
    var progressed = true
    while progressed do
      progressed = false
      remaining.toVector.foreach { case (transactionId, recovered) =>
        recovered.delta.foreach { delta =>
          val shards = delta.updates.map(_.id).toSet
          if recovered.abortedShards.nonEmpty then
            remaining.remove(transactionId): Unit
            progressed = true
          else if shards.subsetOf(recovered.finalizedShards) then
            if CoordinatorShardState.includes(state, delta) then ()
            else
              CoordinatorShardState.merge(state, delta, delta.controllerTerm) match
                case Right(next) => state = next
                case Left(_)     => ()
            if CoordinatorShardState.includes(state, delta) then
              remaining.remove(transactionId): Unit
              progressed = true
          else if shards.subsetOf(recovered.preparedShards) then
            pending.update(transactionId, delta)
            preparedParts.update(transactionId, mutable.HashSet.from(recovered.preparedShards))
            decisionParts.update(transactionId, mutable.HashSet.from(recovered.decidedShards))
            finalizedParts.update(transactionId, mutable.HashSet.from(recovered.finalizedShards))
            if shards.subsetOf(recovered.decidedShards) then decisions += transactionId
            remaining.remove(transactionId): Unit
            progressed = true
          else if recovered.preparedShards.nonEmpty then
            pending.update(transactionId, delta)
            preparedParts.update(transactionId, mutable.HashSet.from(recovered.preparedShards))
            decisionParts.update(transactionId, mutable.HashSet.from(recovered.decidedShards))
            finalizedParts.update(transactionId, mutable.HashSet.from(recovered.finalizedShards))
            remaining.remove(transactionId): Unit
            progressed = true
        }
      }
    if remaining.nonEmpty then
      throw IllegalStateException("coordinator shard journals contain unrecoverable committed transactions")
  }

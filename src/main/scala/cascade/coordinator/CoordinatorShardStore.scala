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
    truncatedBytes: Long = 0L,
    compactions: Long = 0L,
    checkpointBytes: Long = 0L,
    reclaimedBytes: Long = 0L,
    directoryForceSupported: Boolean = false
)

/** Atomic local participant backed by one durable journal per touched shard. */
final class CoordinatorShardStore(
    directory: Path,
    baseline: CoordinatorMetadata,
    compactionBytes: Long = Long.MaxValue
) extends AutoCloseable:
  require(compactionBytes >= 1024L, "coordinator shard compaction threshold must be at least 1 KiB")
  private val root = directory.toAbsolutePath.normalize()
  Files.createDirectories(root)
  private val journals = mutable.HashMap.empty[Int, CoordinatorShardJournal]
  private val pending = mutable.HashMap.empty[CoordinatorTransactionId, CoordinatorDelta]
  private val votes = mutable.HashSet.empty[CoordinatorTransactionId]
  private val certificates = mutable.HashMap.empty[CoordinatorTransactionId, CoordinatorDecisionCertificate]
  private val terminal = mutable.LinkedHashMap.empty[CoordinatorTransactionId, CoordinatorTransactionStatus]
  private val observedAt = mutable.HashMap.empty[CoordinatorTransactionId, Long]
  private val preparedParts = mutable.HashMap.empty[CoordinatorTransactionId, mutable.Set[Int]]
  private val decisionParts = mutable.HashMap.empty[CoordinatorTransactionId, mutable.Set[Int]]
  private val certificateParts = mutable.HashMap.empty[CoordinatorTransactionId, mutable.Set[Int]]
  private val finalizedParts = mutable.HashMap.empty[CoordinatorTransactionId, mutable.Set[Int]]
  private var state = baseline
  private var preparedCount = 0L
  private var decidedCount = 0L
  private var finalizedCount = 0L
  private var abortedCount = 0L
  private var conflictCount = 0L
  recover()

  def metadata: CoordinatorMetadata = synchronized(state)

  /** Advances a checkpoint baseline without overwriting any shard already newer in its independent journal. */
  def installBaseline(metadata: CoordinatorMetadata): Unit = synchronized {
    val equalVersionConflict = Vector.tabulate(CoordinatorShard.Count)(identity).exists { shard =>
      metadata.shardVersion(shard) == state.shardVersion(shard) && metadata.shardPayloads(shard) != state.shardPayloads(shard)
    }
    if equalVersionConflict then throw IllegalArgumentException("coordinator baseline changes an existing shard version")
    val comparisons = Vector.tabulate(CoordinatorShard.Count) { shard =>
      java.lang.Long.compare(metadata.shardVersion(shard), state.shardVersion(shard))
    }
    if comparisons.forall(_ >= 0) then state = metadata
    else if !comparisons.forall(_ <= 0) then
      throw IllegalArgumentException("coordinator baseline crosses independently committed shard versions")
  }

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
        observedAt.update(transactionId, System.currentTimeMillis())
        appendMissing(delta, CoordinatorQuorumRecord.prepare(transactionId, delta), preparedParts, transactionId)
        preparedCount += 1L
        Errors.None
  }

  def decide(transactionId: CoordinatorTransactionId): Short = synchronized {
    pending.get(transactionId) match
      case None => Errors.CoordinatorLoadInProgress
      case Some(delta) =>
        appendMissing(delta, CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Decide), decisionParts, transactionId)
        if complete(transactionId, delta, decisionParts) && !votes(transactionId) then
          votes += transactionId
          decidedCount += 1L
        Errors.None
  }

  def commitDecision(transactionId: CoordinatorTransactionId, certificate: CoordinatorDecisionCertificate): Short = synchronized {
    pending.get(transactionId) match
      case None => Errors.CoordinatorLoadInProgress
      case Some(_) if !votes(transactionId) => Errors.InvalidRequest
      case Some(delta) =>
        certificates.get(transactionId) match
          case Some(existing) if existing != certificate => Errors.InvalidRequest
          case _ =>
            appendMissing(
              delta,
              CoordinatorQuorumRecord.commit(transactionId, certificate),
              certificateParts,
              transactionId
            )
            certificates.update(transactionId, certificate)
            Errors.None
  }

  def recoverCertified(
      transactionId: CoordinatorTransactionId,
      delta: CoordinatorDelta,
      certificate: CoordinatorDecisionCertificate
  ): Short = synchronized {
    pending.get(transactionId) match
      case Some(existing) if existing != delta => Errors.InvalidRequest
      case None if CoordinatorShardState.includes(state, delta) =>
        terminal.update(transactionId, CoordinatorTransactionStatus.Finalized)
        Errors.None
      case None if delta.updates.exists(update => state.shardVersion(update.id) != update.expectedVersion) =>
        conflictCount += 1L
        Errors.CoordinatorLoadInProgress
      case None if pending.valuesIterator.exists(other => overlaps(other, delta)) =>
        conflictCount += 1L
        Errors.CoordinatorLoadInProgress
      case _ =>
        pending.update(transactionId, delta)
        observedAt.getOrElseUpdate(transactionId, System.currentTimeMillis())
        appendMissing(
          delta,
          CoordinatorQuorumRecord.recover(transactionId, delta, certificate),
          certificateParts,
          transactionId
        )
        val shards = mutable.HashSet.from(delta.updates.map(_.id))
        preparedParts.update(transactionId, mutable.HashSet.from(shards))
        decisionParts.update(transactionId, mutable.HashSet.from(shards))
        votes += transactionId
        certificates.update(transactionId, certificate)
        Errors.None
  }

  def finalizeTransaction(transactionId: CoordinatorTransactionId): Either[Short, CoordinatorMetadata] = synchronized {
    pending.get(transactionId) match
      case None => Left(Errors.CoordinatorLoadInProgress)
      case Some(_) if !certificates.contains(transactionId) => Left(Errors.InvalidRequest)
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
            votes -= transactionId
            observedAt.remove(transactionId): Unit
            preparedParts.remove(transactionId): Unit
            decisionParts.remove(transactionId): Unit
            certificateParts.remove(transactionId): Unit
            finalizedParts.remove(transactionId): Unit
            terminal.update(transactionId, CoordinatorTransactionStatus.Finalized)
            finalizedCount += 1L
            compact(delta.updates.map(_.id).toSet)
            Right(next)
  }

  def abort(transactionId: CoordinatorTransactionId): Unit = synchronized {
    pending.remove(transactionId).foreach { delta =>
      appendAll(delta, CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Abort))
      votes -= transactionId
      certificates.remove(transactionId): Unit
      observedAt.remove(transactionId): Unit
      preparedParts.remove(transactionId): Unit
      decisionParts.remove(transactionId): Unit
      certificateParts.remove(transactionId): Unit
      finalizedParts.remove(transactionId): Unit
      terminal.update(transactionId, CoordinatorTransactionStatus.Aborted)
      abortedCount += 1L
      compact(delta.updates.map(_.id).toSet)
    }
  }

  def transactionStatus(transactionId: CoordinatorTransactionId): CoordinatorDecisionQueryResult = synchronized {
    val status = terminal.getOrElse(
      transactionId,
      if certificates.contains(transactionId) then CoordinatorTransactionStatus.Committed
      else if votes(transactionId) then CoordinatorTransactionStatus.Voted
      else if pending.contains(transactionId) then CoordinatorTransactionStatus.Prepared
      else CoordinatorTransactionStatus.Unknown
    )
    CoordinatorDecisionQueryResult(Errors.None, status, certificates.get(transactionId))
  }

  def recoveryCandidates: Vector[CoordinatorRecoveryCandidate] = synchronized {
    pending.toVector.sortBy(entry => (entry._1.high, entry._1.low)).map { case (transactionId, delta) =>
      val status = if certificates.contains(transactionId) then CoordinatorTransactionStatus.Committed
      else if votes(transactionId) then CoordinatorTransactionStatus.Voted
      else CoordinatorTransactionStatus.Prepared
      CoordinatorRecoveryCandidate(
        transactionId,
        status,
        delta,
        certificates.get(transactionId),
        observedAt.getOrElse(transactionId, System.currentTimeMillis())
      )
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
      journal.map(_.truncatedBytes).sum,
      journal.map(_.compactions).sum,
      journal.map(_.checkpointBytes).sum,
      journal.map(_.reclaimedBytes).sum,
      journal.exists(_.directoryForceSupported)
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

  private def compact(shards: Set[Int]): Unit =
    shards.toVector.sorted.foreach { shard =>
      journals.get(shard).filter(_.snapshot.bytes >= compactionBytes).foreach { journal =>
        val checkpoint = CoordinatorShardCheckpoint(
          shard,
          state.shardVersion(shard),
          state.version,
          state.ownerTerm,
          state.shardPayloads(shard)
        )
        val unresolved = pending.toVector.sortBy(entry => (entry._1.high, entry._1.low)).flatMap { case (transactionId, delta) =>
          Option.when(delta.updates.exists(_.id == shard)) {
            certificates.get(transactionId) match
              case Some(certificate) => Vector(CoordinatorQuorumRecord.recover(transactionId, delta, certificate))
              case None if votes(transactionId) => Vector(
                CoordinatorQuorumRecord.prepare(transactionId, delta),
                CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Decide)
              )
              case None => Vector(CoordinatorQuorumRecord.prepare(transactionId, delta))
          }
        }.flatten
        journal.replace(CoordinatorQuorumRecord.checkpoint(checkpoint) +: unresolved)
      }
    }

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

    journals.toVector.sortBy(_._1).foreach { case (_, journal) =>
      journal.entries.flatMap(_.checkpoint).foreach { checkpoint =>
        CoordinatorShardState.installCheckpoint(state, checkpoint) match
          case Right(next) => state = next
          case Left(message) => throw IllegalStateException(message)
      }
    }

    final case class Recovered(
        var delta: Option[CoordinatorDelta] = None,
        preparedShards: mutable.Set[Int] = mutable.HashSet.empty,
        decidedShards: mutable.Set[Int] = mutable.HashSet.empty,
        certifiedShards: mutable.Set[Int] = mutable.HashSet.empty,
        finalizedShards: mutable.Set[Int] = mutable.HashSet.empty,
        abortedShards: mutable.Set[Int] = mutable.HashSet.empty,
        var certificate: Option[CoordinatorDecisionCertificate] = None
    )
    val transactions = mutable.LinkedHashMap.empty[CoordinatorTransactionId, Recovered]
    journals.toVector.sortBy(_._1).foreach { case (shard, journal) =>
      journal.entries.filterNot(_.phase == CoordinatorQuorumPhase.Checkpoint).foreach { record =>
        val recovered = transactions.getOrElseUpdate(record.transactionId, Recovered())
        record.phase match
          case CoordinatorQuorumPhase.Prepare =>
            val delta = record.delta.get
            recovered.delta match
              case Some(existing) if existing != delta => throw IllegalStateException("coordinator transaction payload changed")
              case _ => recovered.delta = Some(delta)
            recovered.preparedShards += shard
          case CoordinatorQuorumPhase.Decide   => recovered.decidedShards += shard
          case CoordinatorQuorumPhase.Commit =>
            val certificate = record.certificate.get
            recovered.certificate match
              case Some(existing) if existing != certificate => throw IllegalStateException("coordinator decision certificate changed")
              case _ => recovered.certificate = Some(certificate)
            recovered.certifiedShards += shard
          case CoordinatorQuorumPhase.Recover =>
            val delta = record.delta.get
            val certificate = record.certificate.get
            recovered.delta match
              case Some(existing) if existing != delta => throw IllegalStateException("coordinator recovery payload changed")
              case _ => recovered.delta = Some(delta)
            recovered.certificate match
              case Some(existing) if existing != certificate => throw IllegalStateException("coordinator decision certificate changed")
              case _ => recovered.certificate = Some(certificate)
            recovered.preparedShards += shard
            recovered.decidedShards += shard
            recovered.certifiedShards += shard
          case CoordinatorQuorumPhase.Finalize => recovered.finalizedShards += shard
          case CoordinatorQuorumPhase.Abort    => recovered.abortedShards += shard
          case CoordinatorQuorumPhase.Checkpoint => ()
      }
    }
    val remaining = mutable.LinkedHashMap.from(transactions)
    var progressed = true
    while progressed do
      progressed = false
      remaining.toVector.foreach { case (transactionId, recovered) =>
        recovered.delta.foreach { delta =>
          val shards = delta.updates.map(_.id).toSet
          if CoordinatorShardState.includes(state, delta) then
            remaining.remove(transactionId): Unit
            progressed = true
          else if recovered.abortedShards.nonEmpty then
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
            observedAt.update(transactionId, System.currentTimeMillis())
            preparedParts.update(transactionId, mutable.HashSet.from(recovered.preparedShards))
            decisionParts.update(transactionId, mutable.HashSet.from(recovered.decidedShards))
            certificateParts.update(transactionId, mutable.HashSet.from(recovered.certifiedShards))
            finalizedParts.update(transactionId, mutable.HashSet.from(recovered.finalizedShards))
            if shards.subsetOf(recovered.decidedShards) then votes += transactionId
            if shards.subsetOf(recovered.certifiedShards) then
              certificates.update(transactionId, recovered.certificate.getOrElse(
                throw IllegalStateException("coordinator certificate record is missing its proof")
              ))
            remaining.remove(transactionId): Unit
            progressed = true
          else if recovered.preparedShards.nonEmpty then
            pending.update(transactionId, delta)
            observedAt.update(transactionId, System.currentTimeMillis())
            preparedParts.update(transactionId, mutable.HashSet.from(recovered.preparedShards))
            decisionParts.update(transactionId, mutable.HashSet.from(recovered.decidedShards))
            certificateParts.update(transactionId, mutable.HashSet.from(recovered.certifiedShards))
            finalizedParts.update(transactionId, mutable.HashSet.from(recovered.finalizedShards))
            remaining.remove(transactionId): Unit
            progressed = true
        }
      }
    if remaining.nonEmpty then
      throw IllegalStateException("coordinator shard journals contain unrecoverable committed transactions")
  }

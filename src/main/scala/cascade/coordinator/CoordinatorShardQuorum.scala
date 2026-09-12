package cascade.coordinator

import cascade.cluster.{ClusterNode, CoordinatorMetadata, QuorumMembership}
import cascade.protocol.Errors
import java.util.concurrent.{Executors, ScheduledExecutorService, Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import scala.util.control.NonFatal

private enum CoordinatorResolutionResult:
  case Recovered, Aborted, Pending

/**
 * Coordinates durable prepare/decision/finalize rounds without entering the metadata controller log.
 * Sorted shard locks serialize overlapping transactions while allowing disjoint shard sets to make progress independently.
 */
final class CoordinatorShardQuorum(
    localNodeId: Int,
    store: CoordinatorShardStore,
    config: CoordinatorQuorumConfig,
    membership: () => QuorumMembership,
    controllerTerm: () => Long,
    replicate: (Vector[ClusterNode], CoordinatorQuorumRecord) => Map[Int, Short],
    install: CoordinatorMetadata => Unit,
    query: (Vector[ClusterNode], CoordinatorTransactionId) => Map[Int, CoordinatorDecisionQueryResult] = (_, _) => Map.empty
) extends AutoCloseable:
  private val closed = AtomicBoolean(false)
  private val admission = Semaphore(config.maxInflightTransactions, true)
  private val shardLocks = Vector.fill(CoordinatorShard.Count)(ReentrantLock(true))
  private val resolver: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-coordinator-resolver").factory())
  private val metricsLock = Object()
  private var inflight = 0
  private var peakInflight = 0
  private var attempts = 0L
  private var committed = 0L
  private var failed = 0L
  private var rejected = 0L
  private var prepareMessages = 0L
  private var decisionMessages = 0L
  private var finalizeMessages = 0L
  private var abortMessages = 0L
  private var certificateMessages = 0L
  private var recoveryMessages = 0L
  private var resolverRuns = 0L
  private var recoveredTransactions = 0L
  private var recoveryAborts = 0L
  private var unresolvedTransactions = 0L
  private var phaseNanos = 0L
  private var recordBytes = 0L
  resolver.scheduleWithFixedDelay(
    () =>
      try resolvePending(): Unit
      catch case NonFatal(error) => System.err.println(s"Cascade coordinator resolution failed: ${error.getMessage}"),
    config.resolutionIntervalMillis,
    config.resolutionIntervalMillis,
    TimeUnit.MILLISECONDS
  ): Unit

  def commit(delta: CoordinatorDelta): Boolean =
    if closed.get() || !acquireAdmission() then
      metricsLock.synchronized(rejected += 1L)
      false
    else
      metricsLock.synchronized {
        inflight += 1
        peakInflight = math.max(peakInflight, inflight)
        attempts += 1L
      }
      val locks = delta.updates.map(_.id).distinct.sorted.map(shardLocks)
      var acquired = 0
      try
        locks.foreach { lock =>
          lock.lockInterruptibly()
          acquired += 1
        }
        val quorum = membership()
        val term = controllerTerm()
        if delta.controllerTerm != term || !quorum.contains(localNodeId) then return complete(success = false)
        val transactionId = CoordinatorTransactionId.random()
        val prepare = CoordinatorQuorumRecord.prepare(transactionId, delta)
        val prepared = runPhase(quorum.voters.map(_.node), prepare, term)
        if !quorum.hasQuorum(prepared) then
          abort(quorum, prepared, transactionId, term)
          return complete(success = false)

        val decision = CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Decide)
        val decided = runPhase(quorum.voters.map(_.node).filter(node => prepared(node.id)), decision, term)
        if !quorum.hasQuorum(decided) then
          abort(quorum, prepared, transactionId, term)
          return complete(success = false)

        val certificate = CoordinatorDecisionCertificate.from(quorum, decided)
        val certified = runPhase(
          quorum.voters.map(_.node).filter(node => decided(node.id)),
          CoordinatorQuorumRecord.commit(transactionId, certificate),
          term
        )
        // A partial certificate is still a durable commit proof. Recovery must finish it;
        // aborting here could contradict a certificate already forced on another voter.
        if !quorum.hasQuorum(certified) then return complete(success = false)

        val finalizeRecord = CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Finalize)
        // The owner remains readable at its last acknowledged image while followers force the
        // terminal record. Publishing locally first would make readiness race ahead of installation.
        val remoteFinalized = runPhase(
          quorum.voters.map(_.node).filter(node => node.id != localNodeId && certified(node.id)),
          finalizeRecord,
          term
        )
        val finalized =
          if certified(localNodeId) && quorum.hasQuorum(remoteFinalized + localNodeId) then
            remoteFinalized ++ runPhase(quorum.voters.map(_.node).filter(_.id == localNodeId), finalizeRecord, term)
          else remoteFinalized
        complete(quorum.hasQuorum(finalized) && finalized(localNodeId))
      catch
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          complete(success = false)
        case NonFatal(_) => complete(success = false)
      finally
        locks.take(acquired).reverse.foreach(_.unlock())
        metricsLock.synchronized(inflight -= 1)
        admission.release()

  def receive(record: CoordinatorQuorumRecord, expectedTerm: Long): Short =
    if closed.get() || expectedTerm != controllerTerm() then Errors.CoordinatorLoadInProgress
    else
      val (code, metadata) = applyLocal(record, expectedTerm)
      metadata.foreach(install)
      code

  def snapshot: CoordinatorQuorumSnapshot = metricsLock.synchronized {
    CoordinatorQuorumSnapshot(
      inflight,
      peakInflight,
      attempts,
      committed,
      failed,
      rejected,
      prepareMessages,
      decisionMessages,
      finalizeMessages,
      abortMessages,
      certificateMessages,
      recoveryMessages,
      resolverRuns,
      recoveredTransactions,
      recoveryAborts,
      unresolvedTransactions,
      phaseNanos,
      recordBytes,
      store.snapshot
    )
  }

  def metadata: CoordinatorMetadata = store.metadata

  def installBaseline(metadata: CoordinatorMetadata): Unit = store.installBaseline(metadata)

  private[cascade] def decisionStates(
      transactionId: CoordinatorTransactionId,
      quorum: QuorumMembership
  ): Map[Int, CoordinatorDecisionQueryResult] =
    val local = Option.when(quorum.contains(localNodeId))(localNodeId -> store.transactionStatus(transactionId)).toMap
    local ++ query(quorum.voters.map(_.node).filterNot(_.id == localNodeId), transactionId)

  /** Resolves durable in-doubt transactions after their original owner stopped making progress. */
  private[cascade] def resolvePending(nowMillis: Long = System.currentTimeMillis()): Int =
    if closed.get() then 0
    else
      val candidates = store.recoveryCandidates.filter(candidate =>
        nowMillis - candidate.observedAtMillis >= config.resolutionDelayMillis
      )
      val results = candidates.map(resolve)
      metricsLock.synchronized {
        resolverRuns += 1L
        recoveredTransactions += results.count(_ == CoordinatorResolutionResult.Recovered).toLong
        recoveryAborts += results.count(_ == CoordinatorResolutionResult.Aborted).toLong
        unresolvedTransactions += results.count(_ == CoordinatorResolutionResult.Pending).toLong
      }
      results.count(_ != CoordinatorResolutionResult.Pending)

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      resolver.shutdownNow(): Unit
      resolver.awaitTermination(5L, TimeUnit.SECONDS): Unit
      store.close()

  private def acquireAdmission(): Boolean =
    try admission.tryAcquire(config.admissionTimeoutMillis, TimeUnit.MILLISECONDS)
    catch
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        false

  private def resolve(candidate: CoordinatorRecoveryCandidate): CoordinatorResolutionResult =
    if !admission.tryAcquire() then CoordinatorResolutionResult.Pending
    else
      val locks = candidate.delta.updates.map(_.id).distinct.sorted.map(shardLocks)
      var acquired = 0
      try
        while acquired < locks.size && locks(acquired).tryLock() do acquired += 1
        if acquired != locks.size then CoordinatorResolutionResult.Pending
        else
          val quorum = membership()
          val term = controllerTerm()
          if !quorum.contains(localNodeId) then CoordinatorResolutionResult.Pending
          else
            val states = decisionStates(candidate.transactionId, quorum)
              .filter(_._2.errorCode == Errors.None)
            val certificate = candidate.certificate.orElse(states.valuesIterator.flatMap(_.certificate).nextOption())
            certificate match
              case Some(proof) =>
                if finishCertified(candidate, proof, quorum, term) then CoordinatorResolutionResult.Recovered
                else CoordinatorResolutionResult.Pending
              case None if quorum.hasQuorum(states.keySet) =>
                val participants = states.collect {
                  case (nodeId, result) if result.status == CoordinatorTransactionStatus.Prepared ||
                      result.status == CoordinatorTransactionStatus.Voted => nodeId
                }.toSet
                abort(quorum, participants, candidate.transactionId, term)
                CoordinatorResolutionResult.Aborted
              case None => CoordinatorResolutionResult.Pending
      catch
        case _: InterruptedException =>
          Thread.currentThread().interrupt()
          CoordinatorResolutionResult.Pending
        case NonFatal(_) => CoordinatorResolutionResult.Pending
      finally
        locks.take(acquired).reverse.foreach(_.unlock())
        admission.release()

  private def finishCertified(
      candidate: CoordinatorRecoveryCandidate,
      certificate: CoordinatorDecisionCertificate,
      quorum: QuorumMembership,
      term: Long
  ): Boolean =
    val recovered = runPhase(
      quorum.voters.map(_.node),
      CoordinatorQuorumRecord.recover(candidate.transactionId, candidate.delta, certificate),
      term
    )
    if !quorum.hasQuorum(recovered) then false
    else
      val finalizeRecord = CoordinatorQuorumRecord.marker(candidate.transactionId, CoordinatorQuorumPhase.Finalize)
      val remote = runPhase(
        quorum.voters.map(_.node).filter(node => node.id != localNodeId && recovered(node.id)),
        finalizeRecord,
        term
      )
      if recovered(localNodeId) && quorum.hasQuorum(remote + localNodeId) then
        val local = runPhase(quorum.voters.map(_.node).filter(_.id == localNodeId), finalizeRecord, term)
        quorum.hasQuorum(remote ++ local) && local(localNodeId)
      else false

  private def runPhase(nodes: Vector[ClusterNode], record: CoordinatorQuorumRecord, term: Long): Set[Int] =
    val started = System.nanoTime()
    val local = nodes.find(_.id == localNodeId).flatMap { _ =>
      val (code, metadata) = applyLocal(record, term)
      metadata.foreach(install)
      Option.when(code == Errors.None)(localNodeId)
    }.toSet
    val remoteNodes = nodes.filterNot(_.id == localNodeId)
    val remote = replicate(remoteNodes, record).collect { case (nodeId, Errors.None) => nodeId }.toSet
    val bytes = CoordinatorQuorumRecordCodec.encode(record).length.toLong
    metricsLock.synchronized {
      record.phase match
        case CoordinatorQuorumPhase.Prepare  => prepareMessages += nodes.size.toLong
        case CoordinatorQuorumPhase.Decide   => decisionMessages += nodes.size.toLong
        case CoordinatorQuorumPhase.Finalize => finalizeMessages += nodes.size.toLong
        case CoordinatorQuorumPhase.Abort    => abortMessages += nodes.size.toLong
        case CoordinatorQuorumPhase.Commit   => certificateMessages += nodes.size.toLong
        case CoordinatorQuorumPhase.Recover  => recoveryMessages += nodes.size.toLong
      recordBytes += bytes * nodes.size.toLong
      phaseNanos += math.max(0L, System.nanoTime() - started)
    }
    local ++ remote

  private def applyLocal(
      record: CoordinatorQuorumRecord,
      term: Long
  ): (Short, Option[CoordinatorMetadata]) =
    try
      record.phase match
        case CoordinatorQuorumPhase.Prepare => store.prepare(record.transactionId, record.delta.get, term) -> None
        case CoordinatorQuorumPhase.Decide  => store.decide(record.transactionId) -> None
        case CoordinatorQuorumPhase.Commit =>
          store.commitDecision(record.transactionId, record.certificate.get) -> None
        case CoordinatorQuorumPhase.Recover =>
          store.recoverCertified(record.transactionId, record.delta.get, record.certificate.get) -> None
        case CoordinatorQuorumPhase.Finalize =>
          store.finalizeTransaction(record.transactionId) match
            case Right(metadata) => Errors.None -> Some(metadata)
            case Left(error)     => error -> None
        case CoordinatorQuorumPhase.Abort =>
          store.abort(record.transactionId)
          Errors.None -> None
    catch case NonFatal(_) => Errors.CoordinatorNotAvailable -> None

  private def abort(
      quorum: QuorumMembership,
      participants: Set[Int],
      transactionId: CoordinatorTransactionId,
      term: Long
  ): Unit =
    if participants.nonEmpty then
      val marker = CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Abort)
      runPhase(quorum.voters.map(_.node).filter(node => participants(node.id)), marker, term): Unit

  private def complete(success: Boolean): Boolean =
    metricsLock.synchronized {
      if success then committed += 1L else failed += 1L
    }
    success

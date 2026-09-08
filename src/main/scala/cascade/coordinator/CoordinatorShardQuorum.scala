package cascade.coordinator

import cascade.cluster.{ClusterNode, CoordinatorMetadata, QuorumMembership}
import cascade.protocol.Errors
import java.util.concurrent.{Semaphore, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import scala.util.control.NonFatal

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
    install: CoordinatorMetadata => Unit
) extends AutoCloseable:
  private val closed = AtomicBoolean(false)
  private val admission = Semaphore(config.maxInflightTransactions, true)
  private val shardLocks = Vector.fill(CoordinatorShard.Count)(ReentrantLock(true))
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
  private var phaseNanos = 0L
  private var recordBytes = 0L

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

        val finalizeRecord = CoordinatorQuorumRecord.marker(transactionId, CoordinatorQuorumPhase.Finalize)
        val finalized = runPhase(quorum.voters.map(_.node).filter(node => decided(node.id)), finalizeRecord, term)
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
      phaseNanos,
      recordBytes,
      store.snapshot
    )
  }

  def metadata: CoordinatorMetadata = store.metadata

  def installBaseline(metadata: CoordinatorMetadata): Unit = store.installBaseline(metadata)

  override def close(): Unit =
    if closed.compareAndSet(false, true) then store.close()

  private def acquireAdmission(): Boolean =
    try admission.tryAcquire(config.admissionTimeoutMillis, TimeUnit.MILLISECONDS)
    catch
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        false

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

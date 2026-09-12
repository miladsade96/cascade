package cascade.coordinator

import cascade.cluster.{ClusterFeature, ClusterManager, CoordinatorMetadata}
import cascade.delivery.DeliveryCoordinator
import cascade.delivery.DeliveryCodec
import cascade.group.{GroupCodec, GroupCoordinator}
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

/** Installs atomic images and submits only changed shards after capability activation. */
final class CoordinatorStateMachine(
    cluster: ClusterManager,
    groups: GroupCoordinator,
    delivery: DeliveryCoordinator
) extends CoordinatorCheckpoint,
      AutoCloseable:
  private val closed = AtomicBoolean(false)
  private val stateLock = Object()
  private var installedVersion = -1L
  private var installed = CoordinatorMetadata.Empty
  private var baseline = Vector.empty[Vector[Byte]]
  private var installedGroupOwnerTerm = -1L
  private val snapshots = CoordinatorSnapshotCache()
  private val metrics = CoordinatorMetrics()
  def metricsSnapshot: CoordinatorMetricsSnapshot = metrics.snapshot
  private val expirationExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-coordinator-expirer").factory())

  cluster.attachCoordinatorInstaller(install)
  groups.attachCheckpoint(() => commitDomain(CoordinatorDomain.Group))
  delivery.attachCheckpoint(() => commitDomain(CoordinatorDomain.Transaction))
  expirationExecutor.scheduleWithFixedDelay(
    () =>
      try
        groups.expireOwned(System.currentTimeMillis(),
          key => cluster.isAssignedCoordinator(CoordinatorKey.group(key)) && !cluster.isBrokerFenced,
          key => cluster.ownsCoordinator(CoordinatorKey.group(key)))
        if cluster.isActiveController then
          delivery.expireNow()
      catch case error: Throwable => System.err.println(s"Cascade coordinator expiration failed: ${error.getMessage}"),
    1L,
    1L,
    TimeUnit.SECONDS
  ): Unit

  override def commit(): Boolean = commitDomain(CoordinatorDomain.Group)

  private def commitDomain(domain: CoordinatorDomain): Boolean = {
    val started = System.nanoTime()
    val (groupImage, acknowledgedGroup) = groups.stagedImages
    val (deliveryImage, acknowledgedDelivery) = delivery.stagedImages
    val candidate = snapshots.capture(groupImage, deliveryImage)
    val acknowledged = snapshots.capture(acknowledgedGroup, acknowledgedDelivery)
    val (base, before, after, intendedShards) = stateLock.synchronized {
      val eligible = domain match
        case CoordinatorDomain.Group       => 0 until CoordinatorShard.Buckets
        case CoordinatorDomain.Transaction => CoordinatorShard.Buckets until CoordinatorShard.Count
      val intended = eligible.filter(shard => candidate.payloads(shard) != acknowledged.payloads(shard)).toVector
      val proposed = baseline.zipWithIndex.map { case (payload, shard) =>
        if intended.contains(shard) then candidate.payloads(shard) else payload
      }
      (installed, baseline, proposed, intended)
    }
    var deltaSize = 0L
    var changedShards = 0
    val fullSize = candidate.fullImageBytes
    val committed =
      try
        if cluster.supportsFeature(ClusterFeature.CoordinatorDeltas) then
          metrics.recordPreparation(candidate, System.nanoTime() - started)
          CoordinatorShardState.changes(base, before, after, cluster.controllerTerm) match
            case Some(delta) =>
              deltaSize = CoordinatorDeltaCodec.encode(delta).length.toLong
              changedShards = delta.updates.size
              cluster.commitCoordinatorDelta(delta)
            case None => !cluster.isBrokerFenced
        else
          stateLock.synchronized {
            cluster.commitCoordinatorState(
              installed.version,
              GroupCodec.encode(groupImage).toVector,
              DeliveryCodec.encode(deliveryImage).toVector
            )
          }
      catch case _: Throwable => false
    if committed then recordLatest(cluster.coordinatorMetadata, intendedShards)
    metrics.record(committed, deltaSize, fullSize, changedShards, System.nanoTime() - started)
    committed
  }

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      expirationExecutor.shutdownNow(): Unit
      expirationExecutor.awaitTermination(5L, TimeUnit.SECONDS): Unit

  private def install(metadata: CoordinatorMetadata): Unit =
    val selected = stateLock.synchronized(selectLatest(metadata).nonEmpty)
    if selected then
      groups.installLatestImage(() => stateLock.synchronized {
        val candidate = installed
        val renewSessions = installedGroupOwnerTerm < 0L || candidate.ownerTerm != installedGroupOwnerTerm
        installedGroupOwnerTerm = candidate.ownerTerm
        candidate.groupImage -> renewSessions
      })
      delivery.installLatestImage(() => stateLock.synchronized(installed.deliveryImage))
      cluster.coordinatorStateInstalled(metadata)

  private def recordLatest(metadata: CoordinatorMetadata, shards: Vector[Int]): Unit = stateLock.synchronized {
    val candidate = if installedVersion < 0L then metadata
    else CoordinatorShardState.mergeMonotonic(installed, metadata).getOrElse(installed)
    installedVersion = candidate.version
    installed = candidate
    baseline = candidate.shardPayloads
    // The calling service already staged these shards. Publish only their readiness;
    // the independent service may still be applying a coalesced image callback.
    cluster.coordinatorShardsInstalled(candidate, shards)
  }

  private def selectLatest(metadata: CoordinatorMetadata): Option[(CoordinatorMetadata, Boolean)] = {
    val candidate =
      if installedVersion < 0L then metadata
      else CoordinatorShardState.mergeMonotonic(installed, metadata).getOrElse(installed)
    val advancesShard = installedVersion < 0L || Vector.tabulate(CoordinatorShard.Count)(identity)
      .exists(shard => candidate.shardVersion(shard) > installed.shardVersion(shard))
    if advancesShard || candidate.ownerTerm > installed.ownerTerm then
      val renewSessions = installedVersion < 0L || candidate.ownerTerm != installed.ownerTerm
      installedVersion = candidate.version
      installed = candidate
      baseline = candidate.shardPayloads
      Some(candidate -> renewSessions)
    else None
  }

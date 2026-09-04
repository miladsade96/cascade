package cascade.coordinator

import cascade.cluster.{ClusterFeature, ClusterManager, CoordinatorMetadata}
import cascade.delivery.DeliveryCoordinator
import cascade.group.GroupCoordinator
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

/** Installs atomic images and submits only changed shards after capability activation. */
final class CoordinatorStateMachine(
    cluster: ClusterManager,
    groups: GroupCoordinator,
    delivery: DeliveryCoordinator,
    stateLock: Object
) extends CoordinatorCheckpoint,
      AutoCloseable:
  private val closed = AtomicBoolean(false)
  private var installedVersion = -1L
  private var installed = CoordinatorMetadata.Empty
  private var baseline = Vector.empty[Vector[Byte]]
  private val snapshots = CoordinatorSnapshotCache()
  private val metrics = CoordinatorMetrics()
  def metricsSnapshot: CoordinatorMetricsSnapshot = metrics.snapshot
  private val expirationExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-coordinator-expirer").factory())

  cluster.attachCoordinatorInstaller(install)
  groups.attachCheckpoint(this)
  delivery.attachCheckpoint(this)
  expirationExecutor.scheduleWithFixedDelay(
    () =>
      try
        groups.expireOwned(System.currentTimeMillis(),
          key => cluster.isAssignedCoordinator(key) && !cluster.isBrokerFenced, cluster.ownsCoordinator)
        if cluster.isActiveController then
          delivery.expireNow()
      catch case error: Throwable => System.err.println(s"Cascade coordinator expiration failed: ${error.getMessage}"),
    1L,
    1L,
    TimeUnit.SECONDS
  ): Unit

  override def commit(): Boolean = stateLock.synchronized {
    val started = System.nanoTime()
    var deltaSize = 0L
    var changedShards = 0
    var fullSize = 0L
    val committed =
      try
        if cluster.supportsFeature(ClusterFeature.CoordinatorDeltas) then
          val candidate = snapshots.capture(groups.image, delivery.image)
          metrics.recordPreparation(candidate, System.nanoTime() - started)
          fullSize = candidate.fullImageBytes
          CoordinatorShardState.changes(installed, baseline, candidate.payloads, cluster.controllerTerm) match
            case Some(delta) =>
              deltaSize = CoordinatorDeltaCodec.encode(delta).length.toLong
              changedShards = delta.updates.size
              cluster.commitCoordinatorDelta(delta)
            case None => !cluster.isBrokerFenced
        else
          val groupState = groups.snapshotBytes.toVector
          val deliveryState = delivery.snapshotBytes.toVector
          fullSize = groupState.size.toLong + deliveryState.size
          cluster.commitCoordinatorState(installed.version, groupState, deliveryState)
      catch case _: Throwable => false
    // A rejected remote proposal may have synchronized a newer image. Never roll that state back.
    installLatest(cluster.coordinatorMetadata, force = true)
    metrics.record(committed, deltaSize, fullSize, changedShards, System.nanoTime() - started)
    committed
  }

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      expirationExecutor.shutdownNow(): Unit
      expirationExecutor.awaitTermination(5L, TimeUnit.SECONDS): Unit

  private def install(metadata: CoordinatorMetadata): Unit = installLatest(metadata, force = false)

  private def installLatest(metadata: CoordinatorMetadata, force: Boolean): Unit = stateLock.synchronized {
    if metadata.version > installedVersion || (force && metadata.version == installedVersion) then
      // Recovery/controller-term changes grant a fresh session window. Ordinary
      // checkpoints are not heartbeats and must not keep abandoned members alive.
      groups.installCommittedImage(metadata.groupImage, renewSessions = installedVersion < 0L || metadata.ownerTerm != installed.ownerTerm)
      delivery.installCommittedImage(metadata.deliveryImage)
      installedVersion = metadata.version
      installed = metadata
      baseline = metadata.shardPayloads
      cluster.coordinatorStateInstalled(metadata)
  }

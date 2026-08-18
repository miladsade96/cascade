package cascade.coordinator

import cascade.cluster.{ClusterManager, CoordinatorMetadata}
import cascade.delivery.DeliveryCoordinator
import cascade.group.GroupCoordinator
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

/** Installs and quorum-commits a single atomic image spanning all coordinator services. */
final class CoordinatorStateMachine(
    cluster: ClusterManager,
    groups: GroupCoordinator,
    delivery: DeliveryCoordinator,
    stateLock: Object
) extends CoordinatorCheckpoint,
      AutoCloseable:
  private val closed = AtomicBoolean(false)
  private val expirationExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-coordinator-expirer").factory())

  cluster.attachCoordinatorInstaller(install)
  groups.attachCheckpoint(this)
  delivery.attachCheckpoint(this)
  expirationExecutor.scheduleWithFixedDelay(
    () =>
      try
        groups.expireNow()
        delivery.expireNow()
      catch case error: Throwable => System.err.println(s"Cascade coordinator expiration failed: ${error.getMessage}"),
    1L,
    1L,
    TimeUnit.SECONDS
  ): Unit

  override def commit(): Boolean = stateLock.synchronized {
    val authoritative = cluster.coordinatorMetadata
    val groupState = groups.snapshotBytes.toVector
    val deliveryState = delivery.snapshotBytes.toVector
    val committed =
      try cluster.commitCoordinatorState(authoritative.version, groupState, deliveryState)
      catch case _: Throwable => false
    if !committed then install(authoritative)
    committed
  }

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      expirationExecutor.shutdownNow(): Unit
      expirationExecutor.awaitTermination(5L, TimeUnit.SECONDS): Unit

  private def install(metadata: CoordinatorMetadata): Unit = stateLock.synchronized {
    groups.installSnapshot(metadata.groupState)
    delivery.installSnapshot(metadata.deliveryState)
  }

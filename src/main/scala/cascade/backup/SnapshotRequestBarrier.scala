package cascade.backup

import java.util.UUID
import java.util.concurrent.TimeUnit

/** A request gate whose freeze token may be released by a different peer-request thread. */
final class SnapshotRequestBarrier(clockNanos: () => Long = () => System.nanoTime()):
  private var activeRequests = 0
  private var owner: Option[UUID] = None
  private var deadlineNanos = 0L
  private var captureCount = 0
  private var leaseNanos = 0L

  def enter(): AutoCloseable = synchronized {
    expireIfEligible()
    while owner.nonEmpty do
      val remaining = deadlineNanos - clockNanos()
      if captureCount == 0 && remaining <= 0L then expireIfEligible()
      else
        val waitMillis = if captureCount > 0 then 1000L else math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining))
        wait(waitMillis)
        expireIfEligible()
    activeRequests = Math.addExact(activeRequests, 1)
    new AutoCloseable:
      private var closed = false
      override def close(): Unit = SnapshotRequestBarrier.this.synchronized {
        if !closed then
          closed = true
          activeRequests -= 1
          if activeRequests == 0 then SnapshotRequestBarrier.this.notifyAll()
      }
  }

  def freeze(snapshotId: UUID, leaseMillis: Long): Unit = synchronized {
    require(leaseMillis > 0L, "snapshot barrier lease must be positive")
    expireIfEligible()
    owner match
      case Some(existing) if existing != snapshotId =>
        throw IllegalStateException(s"snapshot barrier is owned by $existing")
      case _ =>
        owner = Some(snapshotId)
        leaseNanos = TimeUnit.MILLISECONDS.toNanos(leaseMillis)
        deadlineNanos = Math.addExact(clockNanos(), leaseNanos)
    while activeRequests > 0 do
      val remaining = deadlineNanos - clockNanos()
      if remaining <= 0L then
        clearOwner()
        throw IllegalStateException("snapshot barrier lease expired while draining requests")
      wait(math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)))
    assertOwned(snapshotId)
  }

  def beginCapture(snapshotId: UUID): AutoCloseable = synchronized {
    expireIfEligible()
    assertOwned(snapshotId)
    captureCount = Math.addExact(captureCount, 1)
    new AutoCloseable:
      private var closed = false
      override def close(): Unit = SnapshotRequestBarrier.this.synchronized {
        if !closed then
          closed = true
          captureCount -= 1
          if captureCount == 0 then
            deadlineNanos = Math.addExact(clockNanos(), leaseNanos)
            SnapshotRequestBarrier.this.notifyAll()
      }
  }

  def release(snapshotId: UUID): Unit = synchronized {
    assertOwned(snapshotId)
    if captureCount != 0 then throw IllegalStateException("snapshot capture is still active")
    clearOwner()
  }

  def isOwnedBy(snapshotId: UUID): Boolean = synchronized {
    expireIfEligible()
    owner.contains(snapshotId)
  }

  private def assertOwned(snapshotId: UUID): Unit =
    if !owner.contains(snapshotId) then throw IllegalStateException(s"snapshot barrier is not owned by $snapshotId")

  private def expireIfEligible(): Unit =
    if owner.nonEmpty && captureCount == 0 && clockNanos() >= deadlineNanos then clearOwner()

  private def clearOwner(): Unit =
    owner = None
    deadlineNanos = 0L
    leaseNanos = 0L
    notifyAll()

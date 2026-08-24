package cascade.security

import java.nio.file.Path
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.util.control.NonFatal

final class ReloadablePeerIdentities(path: Path, reloadIntervalMillis: Long):
  require(reloadIntervalMillis >= 0L, "peer identity reload interval cannot be negative")

  private val policy = AtomicReference(PeerIdentityFile.load(path))
  private val nextReloadNanos = AtomicLong(deadlineFromNow())
  private val reloadError = AtomicReference(Option.empty[String])

  def authorize(nodeId: Int, principal: String): Boolean =
    reloadIfDue()
    policy.get().authorize(nodeId, principal)

  def nodeIds: Set[Int] =
    reloadIfDue()
    policy.get().nodeIds

  def lastReloadError: Option[String] = reloadError.get()

  def reloadNow(): Boolean = synchronized {
    try
      policy.set(PeerIdentityFile.load(path))
      reloadError.set(None)
      nextReloadNanos.set(deadlineFromNow())
      true
    catch
      case NonFatal(error) =>
        reloadError.set(Some(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))
        nextReloadNanos.set(deadlineFromNow())
        false
  }

  private def reloadIfDue(): Unit =
    val now = System.nanoTime()
    val deadline = nextReloadNanos.get()
    if now >= deadline && nextReloadNanos.compareAndSet(deadline, Long.MaxValue) then reloadNow(): Unit

  private def deadlineFromNow(): Long =
    val intervalNanos = reloadIntervalMillis * 1_000_000L
    val now = System.nanoTime()
    if Long.MaxValue - now < intervalNanos then Long.MaxValue else now + intervalNanos


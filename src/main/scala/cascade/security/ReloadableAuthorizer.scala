package cascade.security

import java.nio.file.Path
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.util.control.NonFatal

final class ReloadableAuthorizer(path: Path, superUsers: Set[String], reloadIntervalMillis: Long):
  require(reloadIntervalMillis >= 0L, "ACL reload interval cannot be negative")

  private val authorizer = AtomicReference(AclAuthorizer.load(path, superUsers))
  private val nextReloadNanos = AtomicLong(deadlineFromNow())
  private val reloadError = AtomicReference(Option.empty[String])

  def authorize(principal: String, operation: AclOperation, resource: Resource): Boolean =
    reloadIfDue()
    authorizer.get().authorize(principal, operation, resource)

  def lastReloadError: Option[String] = reloadError.get()

  def reloadNow(): Boolean = synchronized {
    try
      authorizer.set(AclAuthorizer.load(path, superUsers))
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

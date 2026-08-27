package cascade.security

import java.nio.file.Path
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.util.control.NonFatal

final class ReloadableScramCredentials(path: Path, reloadIntervalMillis: Long):
  require(reloadIntervalMillis >= 0L, "SCRAM credential reload interval cannot be negative")

  private val credentials = AtomicReference(ScramCredentialFile.load(path))
  private val nextReloadNanos = AtomicLong(deadlineFromNow())
  private val reloadError = AtomicReference(Option.empty[String])

  def credential(mechanism: SaslMechanism, user: String): Option[ScramCredential] =
    reloadIfDue()
    credentials.get().credential(mechanism, user)

  def principals: Set[String] =
    reloadIfDue()
    credentials.get().principals

  def mechanisms: Set[SaslMechanism] =
    reloadIfDue()
    credentials.get().mechanisms

  def lastReloadError: Option[String] =
    reloadIfDue()
    reloadError.get()

  def reloadNow(): Boolean = synchronized {
    try
      credentials.set(ScramCredentialFile.load(path))
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

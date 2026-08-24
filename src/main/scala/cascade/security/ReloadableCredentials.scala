package cascade.security

import java.nio.file.Path
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import scala.util.control.NonFatal

final class ReloadableCredentials(path: Path, reloadIntervalMillis: Long):
  require(reloadIntervalMillis >= 0L, "credential reload interval cannot be negative")

  private val credentials = AtomicReference(CredentialFile.load(path))
  private val nextReloadNanos = AtomicLong(deadlineFromNow())
  private val reloadError = AtomicReference(Option.empty[String])

  def authenticate(user: String, password: Array[Char]): Boolean =
    reloadIfDue()
    credentials.get().get(user).exists(_.verify(password))

  def principals: Set[String] =
    reloadIfDue()
    credentials.get().keySet

  def lastReloadError: Option[String] = reloadError.get()

  def reloadNow(): Boolean = synchronized {
    try
      credentials.set(CredentialFile.load(path))
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
    if now >= nextReloadNanos.get() && nextReloadNanos.compareAndSet(nextReloadNanos.get(), Long.MaxValue) then
      reloadNow(): Unit

  private def deadlineFromNow(): Long =
    val intervalNanos = reloadIntervalMillis * 1_000_000L
    val now = System.nanoTime()
    if Long.MaxValue - now < intervalNanos then Long.MaxValue else now + intervalNanos

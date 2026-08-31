package cascade.security

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import javax.net.ssl.SSLContext

final case class TlsContextSnapshot(context: SSLContext, generation: Long)

final case class TlsReloadSnapshot(
    enabled: Boolean,
    generation: Long,
    successfulReloads: Long,
    failedReloads: Long
)

object TlsReloadSnapshot:
  val Empty: TlsReloadSnapshot = TlsReloadSnapshot(false, 0L, 0L, 0L)

final class ReloadableTlsContext(
    config: TlsConfig,
    onReload: TlsContextSnapshot => Unit = _ => (),
    onFailure: Throwable => Unit = _ => ()
) extends AutoCloseable:
  private final case class Active(context: SSLContext, fingerprint: String, generation: Long)

  private val initial = loadInitial()
  private val active = AtomicReference(initial)
  private val successfulReloads = AtomicLong(0L)
  private val failedReloads = AtomicLong(0L)
  private val reloadFailure = AtomicReference[Option[String]](None)
  private var lastAttemptedFingerprint: Option[String] = Some(initial.fingerprint)
  private var lastReadFailure: Option[String] = None
  private val scheduler: Option[ScheduledExecutorService] = Option.when(config.reloadIntervalMillis > 0L) {
    Executors.newSingleThreadScheduledExecutor(
      Thread.ofPlatform().daemon().name("cascade-tls-material-reloader").factory()
    )
  }
  scheduler.foreach(
    _.scheduleWithFixedDelay(
      () => reloadSafely(),
      config.reloadIntervalMillis,
      config.reloadIntervalMillis,
      TimeUnit.MILLISECONDS
    ): Unit
  )

  def current: TlsContextSnapshot =
    val value = active.get()
    TlsContextSnapshot(value.context, value.generation)

  def lastReloadError: Option[String] = reloadFailure.get()

  def snapshot: TlsReloadSnapshot =
    val value = active.get()
    TlsReloadSnapshot(
      enabled = true,
      generation = value.generation,
      successfulReloads = successfulReloads.get(),
      failedReloads = failedReloads.get()
    )

  /** Checks the configured stores immediately. Returns true only when a new context is installed. */
  def reloadNow(): Boolean = synchronized {
    var material: TlsMaterial | Null = null
    try
      material = TlsContextFactory.readMaterial(config)
      val fingerprint = material.fingerprint
      lastReadFailure = None
      if lastAttemptedFingerprint.contains(fingerprint) then false
      else
        lastAttemptedFingerprint = Some(fingerprint)
        val currentValue = active.get()
        if currentValue.fingerprint == fingerprint then
          reloadFailure.set(None)
          false
        else
          try
            val next = Active(material.createContext(), fingerprint, Math.addExact(currentValue.generation, 1L))
            active.set(next)
            reloadFailure.set(None)
            successfulReloads.incrementAndGet(): Unit
            notifyReload(TlsContextSnapshot(next.context, next.generation))
            true
          catch
            case error: Throwable =>
              recordFailure(error)
              false
    catch
      case error: Throwable =>
        lastAttemptedFingerprint = None
        val detail = describe(error)
        if !lastReadFailure.contains(detail) then
          lastReadFailure = Some(detail)
          recordFailure(error)
        else reloadFailure.set(Some(detail))
        false
    finally if material != null then material.close()
  }

  override def close(): Unit =
    scheduler.foreach { executor =>
      executor.shutdownNow(): Unit
      executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
    }

  private def loadInitial(): Active =
    val material = TlsContextFactory.readMaterial(config)
    try Active(material.createContext(), material.fingerprint, 0L)
    finally material.close()

  private def reloadSafely(): Unit =
    try reloadNow(): Unit
    catch case _: Throwable => ()

  private def recordFailure(error: Throwable): Unit =
    reloadFailure.set(Some(describe(error)))
    failedReloads.incrementAndGet(): Unit
    try onFailure(error)
    catch case _: Throwable => ()

  private def notifyReload(snapshot: TlsContextSnapshot): Unit =
    try onReload(snapshot)
    catch case _: Throwable => ()

  private def describe(error: Throwable): String =
    val message = Option(error.getMessage).map(_.trim).filter(_.nonEmpty).getOrElse("TLS material reload failed")
    s"${error.getClass.getSimpleName}: $message"

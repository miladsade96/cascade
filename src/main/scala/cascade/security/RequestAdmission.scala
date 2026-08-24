package cascade.security

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}

final case class RequestAdmissionSnapshot(active: Int, rejected: Long)

final class RequestAdmission(maxInFlight: Int):
  require(maxInFlight > 0, "maximum in-flight requests must be positive")

  private val permits = Semaphore(maxInFlight)
  private val active = AtomicInteger(0)
  private val rejected = AtomicLong(0L)

  def tryAcquire(): Option[RequestLease] =
    if permits.tryAcquire() then
      active.incrementAndGet(): Unit
      Some(RequestLease(() => release()))
    else
      rejected.incrementAndGet(): Unit
      None

  def snapshot: RequestAdmissionSnapshot = RequestAdmissionSnapshot(active.get(), rejected.get())

  private def release(): Unit =
    active.decrementAndGet(): Unit
    permits.release()

final class RequestLease private[security] (release: () => Unit) extends AutoCloseable:
  private val closed = AtomicBoolean(false)
  override def close(): Unit = if closed.compareAndSet(false, true) then release()

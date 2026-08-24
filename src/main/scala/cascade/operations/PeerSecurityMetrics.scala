package cascade.operations

import java.util.concurrent.atomic.AtomicLong

final case class PeerSecuritySnapshot(
    authenticated: Long,
    tlsAuthenticated: Long,
    rejected: Long
)

object PeerSecuritySnapshot:
  val Empty: PeerSecuritySnapshot = PeerSecuritySnapshot(0L, 0L, 0L)

final class PeerSecurityMetrics:
  private val authenticated = AtomicLong(0L)
  private val tlsAuthenticated = AtomicLong(0L)
  private val rejected = AtomicLong(0L)

  def recordAuthenticated(encrypted: Boolean): Unit =
    authenticated.incrementAndGet(): Unit
    if encrypted then tlsAuthenticated.incrementAndGet(): Unit

  def recordRejected(): Unit = rejected.incrementAndGet(): Unit

  def snapshot: PeerSecuritySnapshot =
    PeerSecuritySnapshot(authenticated.get(), tlsAuthenticated.get(), rejected.get())

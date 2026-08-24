package cascade.security

import java.util.concurrent.atomic.AtomicBoolean

final case class ConnectionAdmissionSnapshot(active: Int, rejected: Long, activeByIp: Map[String, Int])

final class ConnectionAdmission(maxConnections: Int, maxConnectionsPerIp: Int):
  require(maxConnections > 0, "maximum connections must be positive")
  require(maxConnectionsPerIp > 0, "maximum connections per IP must be positive")

  private var active = 0
  private var rejected = 0L
  private var byIp = Map.empty[String, Int]

  def tryAcquire(ipAddress: String): Option[ConnectionLease] = synchronized {
    val ipConnections = byIp.getOrElse(ipAddress, 0)
    if active >= maxConnections || ipConnections >= maxConnectionsPerIp then
      rejected += 1L
      None
    else
      active += 1
      byIp = byIp.updated(ipAddress, ipConnections + 1)
      Some(ConnectionLease(() => release(ipAddress)))
  }

  def snapshot: ConnectionAdmissionSnapshot = synchronized(ConnectionAdmissionSnapshot(active, rejected, byIp))

  private def release(ipAddress: String): Unit = synchronized {
    val remaining = byIp.getOrElse(ipAddress, 1) - 1
    if remaining <= 0 then byIp -= ipAddress else byIp = byIp.updated(ipAddress, remaining)
    active = math.max(0, active - 1)
  }

final class ConnectionLease private[security] (release: () => Unit) extends AutoCloseable:
  private val closed = AtomicBoolean(false)
  override def close(): Unit = if closed.compareAndSet(false, true) then release()

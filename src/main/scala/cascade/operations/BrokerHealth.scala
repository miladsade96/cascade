package cascade.operations

final case class HealthPolicy(
    maxPendingFlushBytes: Long,
    minimumUsableDiskBytes: Long
):
  require(maxPendingFlushBytes >= 0L, "maximum pending flush bytes cannot be negative")
  require(minimumUsableDiskBytes >= 0L, "minimum usable disk bytes cannot be negative")

final case class HealthCheck(name: String, healthy: Boolean, detail: String)

final case class BrokerHealth(live: Boolean, ready: Boolean, checks: Vector[HealthCheck]):
  def failedChecks: Vector[HealthCheck] = checks.filterNot(_.healthy)

object BrokerHealth:
  def evaluate(
      snapshot: BrokerMetricsSnapshot,
      policy: HealthPolicy,
      structuredLogFailure: Option[String]
  ): BrokerHealth =
    val checks = Vector(
      HealthCheck("broker_running", snapshot.running, if snapshot.running then "running" else "stopped"),
      HealthCheck("broker_unfenced", !snapshot.brokerFenced, if snapshot.brokerFenced then "fenced" else "unfenced"),
      HealthCheck(
        "flush_backlog",
        snapshot.pendingFlushBytes <= policy.maxPendingFlushBytes,
        s"${snapshot.pendingFlushBytes}/${policy.maxPendingFlushBytes} bytes"
      ),
      HealthCheck(
        "disk_reserve",
        snapshot.usableDiskBytes >= policy.minimumUsableDiskBytes,
        s"${snapshot.usableDiskBytes}/${policy.minimumUsableDiskBytes} bytes"
      ),
      HealthCheck(
        "structured_log",
        structuredLogFailure.isEmpty,
        structuredLogFailure.getOrElse("available")
      )
    )
    BrokerHealth(live = snapshot.running, ready = checks.forall(_.healthy), checks = checks)


package cascade.operations

final case class CapacityLimits(maxConnections: Int, maxInFlightRequests: Int):
  require(maxConnections > 0, "maximum connections must be positive")
  require(maxInFlightRequests > 0, "maximum in-flight requests must be positive")

final case class CapacityAlert(code: String, current: Long, threshold: Long, unit: String):
  def fields: Map[String, String] = Map(
    "alert" -> code,
    "current" -> current.toString,
    "threshold" -> threshold.toString,
    "unit" -> unit
  )

object CapacityAlerts:
  def evaluate(
      snapshot: BrokerMetricsSnapshot,
      limits: CapacityLimits,
      config: CapacityAlertConfig
  ): Vector[CapacityAlert] =
    val connectionThreshold = utilizationThreshold(limits.maxConnections, config.connectionUtilization)
    val requestThreshold = utilizationThreshold(limits.maxInFlightRequests, config.inFlightUtilization)
    Vector(
      Option.when(snapshot.activeConnections.toLong >= connectionThreshold)(
        CapacityAlert("connections_near_limit", snapshot.activeConnections.toLong, connectionThreshold, "connections")
      ),
      Option.when(snapshot.activeRequests.toLong >= requestThreshold)(
        CapacityAlert("requests_near_limit", snapshot.activeRequests.toLong, requestThreshold, "requests")
      ),
      Option.when(config.pendingFlushBytes > 0L && snapshot.pendingFlushBytes >= config.pendingFlushBytes)(
        CapacityAlert("flush_backlog_high", snapshot.pendingFlushBytes, config.pendingFlushBytes, "bytes")
      ),
      Option.when(config.minimumFreeBytes > 0L && snapshot.usableDiskBytes < config.minimumFreeBytes)(
        CapacityAlert("disk_space_low", snapshot.usableDiskBytes, config.minimumFreeBytes, "bytes")
      )
    ).flatten

  private def utilizationThreshold(limit: Int, ratio: Double): Long =
    math.max(1L, math.ceil(limit.toDouble * ratio).toLong)


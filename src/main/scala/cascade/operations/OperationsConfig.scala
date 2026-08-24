package cascade.operations

import java.nio.file.Path

final case class CapacityAlertConfig(
    intervalMillis: Long = 30_000L,
    connectionUtilization: Double = 0.85d,
    inFlightUtilization: Double = 0.85d,
    pendingFlushBytes: Long = 512L * 1024 * 1024,
    minimumFreeBytes: Long = 0L,
    repeatIntervalMillis: Long = 5L * 60 * 1000
):
  require(intervalMillis > 0L, "capacity alert interval must be positive")
  require(connectionUtilization > 0d && connectionUtilization <= 1d, "connection alert utilization must be in (0, 1]")
  require(inFlightUtilization > 0d && inFlightUtilization <= 1d, "in-flight alert utilization must be in (0, 1]")
  require(pendingFlushBytes >= 0L, "pending flush alert threshold cannot be negative")
  require(minimumFreeBytes >= 0L, "free-space alert threshold cannot be negative")
  require(repeatIntervalMillis > 0L, "capacity alert repeat interval must be positive")

final case class OperationsConfig(
    bindHost: String = "127.0.0.1",
    port: Option[Int] = None,
    authenticationToken: Option[String] = None,
    structuredLog: Option[Path] = None,
    structuredLogMaxBytes: Long = 64L * 1024 * 1024,
    structuredLogRetainedFiles: Int = 5,
    logToStderr: Boolean = true,
    readinessMaxPendingFlushBytes: Long = Long.MaxValue,
    capacityAlerts: CapacityAlertConfig = CapacityAlertConfig()
):
  require(bindHost.nonEmpty, "operations bind host cannot be empty")
  require(port.forall(value => value >= 0 && value <= 65535), "operations port must be between 0 and 65535")
  require(authenticationToken.forall(_.length >= 32), "operations authentication token must contain at least 32 characters")
  require(structuredLogMaxBytes >= 1024L, "structured log size must be at least 1 KiB")
  require(structuredLogRetainedFiles > 0, "structured log retention must be positive")
  require(readinessMaxPendingFlushBytes >= 0L, "readiness pending-flush threshold cannot be negative")
  def enabled: Boolean = port.nonEmpty

  def validate(): OperationsConfig =
    require(
      port.isEmpty || isLoopback(bindHost) || authenticationToken.nonEmpty,
      "a non-loopback operations listener requires an authentication token"
    )
    this

  private def isLoopback(host: String): Boolean =
    Set("127.0.0.1", "::1", "0:0:0:0:0:0:0:1", "localhost").contains(host.toLowerCase)

package cascade.security

import java.nio.file.Path

enum SecurityProtocol(val tls: Boolean, val sasl: Boolean):
  case Plaintext extends SecurityProtocol(false, false)
  case Ssl extends SecurityProtocol(true, false)
  case SaslPlaintext extends SecurityProtocol(false, true)
  case SaslSsl extends SecurityProtocol(true, true)

object SecurityProtocol:
  def parse(value: String): SecurityProtocol = value.trim.toUpperCase match
    case "PLAINTEXT"      => SecurityProtocol.Plaintext
    case "SSL"            => SecurityProtocol.Ssl
    case "SASL_PLAINTEXT" => SecurityProtocol.SaslPlaintext
    case "SASL_SSL"       => SecurityProtocol.SaslSsl
    case other             => throw IllegalArgumentException(s"unsupported security protocol: $other")

enum TlsClientAuth:
  case None, Requested, Required

object TlsClientAuth:
  def parse(value: String): TlsClientAuth = value.trim.toLowerCase match
    case "none"      => TlsClientAuth.None
    case "requested" => TlsClientAuth.Requested
    case "required"  => TlsClientAuth.Required
    case other        => throw IllegalArgumentException(s"unsupported TLS client authentication mode: $other")

final case class TlsConfig(
    keyStore: Option[Path] = None,
    keyStorePassword: Option[String] = None,
    keyPassword: Option[String] = None,
    trustStore: Option[Path] = None,
    trustStorePassword: Option[String] = None,
    clientAuth: TlsClientAuth = TlsClientAuth.None,
    enabledProtocols: Vector[String] = Vector("TLSv1.3", "TLSv1.2")
)

final case class AuthenticationConfig(
    credentialsFile: Option[Path] = None,
    reloadIntervalMillis: Long = 1000L,
    sessionLifetimeMillis: Long = 0L
):
  require(reloadIntervalMillis >= 0L, "credential reload interval cannot be negative")
  require(sessionLifetimeMillis >= 0L, "SASL session lifetime cannot be negative")

final case class AuthorizationConfig(
    aclFile: Option[Path] = None,
    reloadIntervalMillis: Long = 1000L,
    superUsers: Set[String] = Set.empty
):
  require(reloadIntervalMillis >= 0L, "ACL reload interval cannot be negative")

final case class AuditConfig(path: Option[Path] = None, forceEachEvent: Boolean = true)

final case class ResourceLimits(
    maxConnections: Int = 10000,
    maxConnectionsPerIp: Int = 1000,
    maxInFlightRequests: Int = 10000,
    requestBytesPerSecond: Long = 0L,
    requestBurstBytes: Long = 0L,
    maxThrottleMillis: Long = 1000L
):
  require(maxConnections > 0, "maximum connections must be positive")
  require(maxConnectionsPerIp > 0, "maximum connections per IP must be positive")
  require(maxInFlightRequests > 0, "maximum in-flight requests must be positive")
  require(requestBytesPerSecond >= 0L, "request quota cannot be negative")
  require(requestBurstBytes >= 0L, "request quota burst cannot be negative")
  require(maxThrottleMillis >= 0L, "maximum throttle cannot be negative")

final case class BrokerSecurityConfig(
    protocol: SecurityProtocol = SecurityProtocol.Plaintext,
    tls: TlsConfig = TlsConfig(),
    authentication: AuthenticationConfig = AuthenticationConfig(),
    authorization: AuthorizationConfig = AuthorizationConfig(),
    audit: AuditConfig = AuditConfig(),
    resources: ResourceLimits = ResourceLimits()
):
  require(!protocol.tls || tls.keyStore.nonEmpty, "TLS requires a key store")
  require(!protocol.tls || tls.keyStorePassword.nonEmpty, "TLS requires a key-store password")
  require(!protocol.sasl || authentication.credentialsFile.nonEmpty, "SASL requires a credentials file")
  require(
    tls.clientAuth == TlsClientAuth.None || tls.trustStore.nonEmpty,
    "TLS client authentication requires a trust store"
  )

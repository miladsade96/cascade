package cascade.security

import java.net.URI
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

enum SaslMechanism(val wireName: String, val scram: Boolean):
  case Plain extends SaslMechanism("PLAIN", false)
  case ScramSha256 extends SaslMechanism("SCRAM-SHA-256", true)
  case ScramSha512 extends SaslMechanism("SCRAM-SHA-512", true)
  case OAuthBearer extends SaslMechanism("OAUTHBEARER", false)

  def oauth: Boolean = this == SaslMechanism.OAuthBearer

object SaslMechanism:
  val Supported: Vector[SaslMechanism] = Vector(Plain, ScramSha256, ScramSha512, OAuthBearer)

  def parse(value: String): SaslMechanism =
    Supported.find(_.wireName == value.trim.toUpperCase).getOrElse {
      throw IllegalArgumentException(s"unsupported SASL mechanism: ${value.trim}")
    }

enum TlsClientAuth:
  case None, Requested, Required

object TlsClientAuth:
  def parse(value: String): TlsClientAuth = value.trim.toLowerCase match
    case "none"      => TlsClientAuth.None
    case "requested" => TlsClientAuth.Requested
    case "required"  => TlsClientAuth.Required
    case other        => throw IllegalArgumentException(s"unsupported TLS client authentication mode: $other")

enum PeerSecurityProtocol(val tls: Boolean):
  case Plaintext extends PeerSecurityProtocol(false)
  case Ssl extends PeerSecurityProtocol(true)

object PeerSecurityProtocol:
  def parse(value: String): PeerSecurityProtocol = value.trim.toUpperCase match
    case "PLAINTEXT" => PeerSecurityProtocol.Plaintext
    case "SSL"       => PeerSecurityProtocol.Ssl
    case other        => throw IllegalArgumentException(s"unsupported peer security protocol: $other")

final case class TlsConfig(
    keyStore: Option[Path] = None,
    keyStorePassword: Option[String] = None,
    keyPassword: Option[String] = None,
    trustStore: Option[Path] = None,
    trustStorePassword: Option[String] = None,
    clientAuth: TlsClientAuth = TlsClientAuth.None,
    enabledProtocols: Vector[String] = Vector("TLSv1.3", "TLSv1.2"),
    reloadIntervalMillis: Long = 1000L
):
  require(enabledProtocols.nonEmpty, "at least one TLS protocol must be enabled")
  require(enabledProtocols.distinct.size == enabledProtocols.size, "TLS protocols must be unique")
  require(reloadIntervalMillis >= 0L, "TLS reload interval cannot be negative")

enum JwtAlgorithm(val jwtName: String, val signatureName: String):
  case Rs256 extends JwtAlgorithm("RS256", "SHA256withRSA")
  case Rs384 extends JwtAlgorithm("RS384", "SHA384withRSA")
  case Rs512 extends JwtAlgorithm("RS512", "SHA512withRSA")

object JwtAlgorithm:
  val Supported: Vector[JwtAlgorithm] = Vector(Rs256, Rs384, Rs512)

  def parse(value: String): JwtAlgorithm =
    Supported.find(_.jwtName == value.trim.toUpperCase).getOrElse {
      throw IllegalArgumentException(s"unsupported JWT algorithm: ${value.trim}")
    }

final case class OAuthConfig(
    jwksUri: Option[URI] = None,
    issuer: Option[String] = None,
    audience: Option[String] = None,
    principalClaim: String = "sub",
    scopeClaim: String = "scope",
    roleClaim: Option[String] = None,
    roleMappings: Map[String, String] = Map.empty,
    requiredScopes: Set[String] = Set.empty,
    allowedAlgorithms: Set[JwtAlgorithm] = Set(JwtAlgorithm.Rs256),
    clockSkewSeconds: Long = 30L,
    jwksRefreshMillis: Long = 300000L,
    httpTimeoutMillis: Int = 5000,
    maximumTokenBytes: Int = 16 * 1024
):
  require(issuer.forall(value => value.nonEmpty && value.length <= 2048 && !value.exists(_.isControl)), "OAuth issuer is invalid")
  require(audience.forall(value => value.nonEmpty && value.length <= 512 && !value.exists(_.isControl)), "OAuth audience is invalid")
  require(principalClaim.nonEmpty && principalClaim.length <= 128, "OAuth principal claim must contain 1-128 characters")
  require(scopeClaim.nonEmpty && scopeClaim.length <= 128, "OAuth scope claim must contain 1-128 characters")
  require(roleClaim.forall(value => value.nonEmpty && value.length <= 128 && !value.exists(_.isWhitespace)), "OAuth role claim is invalid")
  require(
    roleMappings.forall { case (claim, role) =>
      claim.nonEmpty && claim.length <= 256 && !claim.exists(_.isWhitespace) &&
        role.nonEmpty && role.length <= 128 && !role.exists(character => character.isWhitespace || character.isControl)
    },
    "OAuth role mappings are invalid"
  )
  require(requiredScopes.forall(scope => scope.nonEmpty && scope.length <= 256 && !scope.exists(_.isWhitespace)), "OAuth scopes are invalid")
  require(allowedAlgorithms.nonEmpty, "at least one JWT algorithm must be allowed")
  require(clockSkewSeconds >= 0L && clockSkewSeconds <= 300L, "OAuth clock skew must be between 0 and 300 seconds")
  require(jwksRefreshMillis >= 0L, "JWKS refresh interval cannot be negative")
  require(httpTimeoutMillis >= 100 && httpTimeoutMillis <= 60000, "OAuth HTTP timeout must be between 100 and 60000 milliseconds")
  require(maximumTokenBytes >= 1024 && maximumTokenBytes <= 1024 * 1024, "OAuth token limit must be between 1 KiB and 1 MiB")
  jwksUri.foreach { uri =>
    require(uri.isAbsolute, "OAuth JWKS URI must be absolute")
    val scheme = Option(uri.getScheme).map(_.toLowerCase).orNull
    require(Set("file", "https").contains(scheme), "OAuth JWKS URI must use file or https")
    require(uri.getFragment == null && uri.getUserInfo == null, "OAuth JWKS URI cannot contain user info or a fragment")
    if scheme == "file" then
      require(uri.getAuthority == null && uri.getQuery == null, "OAuth file JWKS URI cannot contain an authority or query")
    else require(Option(uri.getHost).exists(_.nonEmpty), "OAuth HTTPS JWKS URI must contain a host")
  }

final case class AuthenticationConfig(
    credentialsFile: Option[Path] = None,
    scramCredentialsFile: Option[Path] = None,
    mechanisms: Vector[SaslMechanism] = Vector(SaslMechanism.Plain),
    oauth: OAuthConfig = OAuthConfig(),
    reloadIntervalMillis: Long = 1000L,
    sessionLifetimeMillis: Long = 0L
):
  require(mechanisms.nonEmpty, "at least one SASL mechanism must be enabled")
  require(mechanisms.distinct.size == mechanisms.size, "SASL mechanisms must be unique")
  require(reloadIntervalMillis >= 0L, "credential reload interval cannot be negative")
  require(sessionLifetimeMillis >= 0L, "SASL session lifetime cannot be negative")

final case class AuthorizationConfig(
    aclFile: Option[Path] = None,
    reloadIntervalMillis: Long = 1000L,
    superUsers: Set[String] = Set.empty
):
  require(reloadIntervalMillis >= 0L, "ACL reload interval cannot be negative")

final case class AuditConfig(path: Option[Path] = None, forceEachEvent: Boolean = true)

final case class PeerSecurityConfig(
    protocol: PeerSecurityProtocol = PeerSecurityProtocol.Plaintext,
    identityFile: Option[Path] = None,
    identityReloadIntervalMillis: Long = 1000L,
    endpointIdentificationAlgorithm: String = "HTTPS"
):
  require(identityReloadIntervalMillis >= 0L, "peer identity reload interval cannot be negative")
  require(endpointIdentificationAlgorithm == "HTTPS", "peer TLS endpoint identification must use HTTPS hostname verification")

  def validate(listenerProtocol: SecurityProtocol, tls: TlsConfig): PeerSecurityConfig =
    if protocol.tls then
      require(listenerProtocol.tls, "peer SSL requires an SSL or SASL_SSL broker listener")
      require(tls.clientAuth != TlsClientAuth.None, "peer SSL requires requested or required TLS client authentication")
      require(tls.trustStore.nonEmpty, "peer SSL requires a trust store")
      require(tls.trustStorePassword.nonEmpty, "peer SSL requires a trust-store password")
      require(identityFile.nonEmpty, "peer SSL requires a peer identity file")
    else require(identityFile.isEmpty, "a peer identity file requires peer SSL")
    this

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
    resources: ResourceLimits = ResourceLimits(),
    peer: PeerSecurityConfig = PeerSecurityConfig()
):
  def validate(): BrokerSecurityConfig =
    require(!protocol.tls || tls.keyStore.nonEmpty, "TLS requires a key store")
    require(!protocol.tls || tls.keyStorePassword.nonEmpty, "TLS requires a key-store password")
    require(
      !protocol.sasl || !authentication.mechanisms.contains(SaslMechanism.Plain) || authentication.credentialsFile.nonEmpty,
      "SASL PLAIN requires a credentials file"
    )
    require(
      !protocol.sasl || !authentication.mechanisms.exists(_.scram) || authentication.scramCredentialsFile.nonEmpty,
      "SASL SCRAM requires a SCRAM credentials file"
    )
    val oauthEnabled = protocol.sasl && authentication.mechanisms.exists(_.oauth)
    require(
      authentication.oauth.roleMappings.isEmpty == authentication.oauth.roleClaim.isEmpty,
      "OAuth role claim and role mappings must be configured together"
    )
    require(!oauthEnabled || protocol == SecurityProtocol.SaslSsl, "SASL OAUTHBEARER requires SASL_SSL")
    require(!oauthEnabled || authentication.oauth.jwksUri.nonEmpty, "SASL OAUTHBEARER requires a JWKS URI")
    require(!oauthEnabled || authentication.oauth.issuer.exists(_.nonEmpty), "SASL OAUTHBEARER requires an issuer")
    require(!oauthEnabled || authentication.oauth.audience.exists(_.nonEmpty), "SASL OAUTHBEARER requires an audience")
    require(
      tls.clientAuth == TlsClientAuth.None || tls.trustStore.nonEmpty,
      "TLS client authentication requires a trust store"
    )
    peer.validate(protocol, tls): Unit
    this

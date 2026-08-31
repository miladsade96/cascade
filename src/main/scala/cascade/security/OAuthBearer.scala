package cascade.security

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.security.Signature
import java.time.Clock
import java.util.Base64
import scala.util.control.NoStackTrace
import scala.util.control.NonFatal

final case class OAuthIdentity(principal: String, expiresAtEpochMillis: Long, scopes: Set[String], roles: Set[String] = Set.empty)

enum JwtValidationError:
  case Malformed, UnsupportedAlgorithm, UnknownKey, InvalidSignature, InvalidIssuer, InvalidAudience,
      Expired, NotYetValid, InvalidPrincipal, MissingScope

private final case class JwtRejected(error: JwtValidationError) extends RuntimeException with NoStackTrace

final class JwtValidator(config: OAuthConfig, keys: ReloadableJwks, clock: Clock = Clock.systemUTC()):
  def validate(token: String): Either[JwtValidationError, OAuthIdentity] =
    try Right(validateUnsafe(token))
    catch
      case rejection: JwtRejected => Left(rejection.error)
      case NonFatal(_)             => Left(JwtValidationError.Malformed)

  private def validateUnsafe(token: String): OAuthIdentity =
    if token.isEmpty || token.length > config.maximumTokenBytes || token.exists(character => character > 0x7f) then
      reject(JwtValidationError.Malformed)
    val segments = token.split("\\.", -1)
    if segments.length != 3 || segments.exists(_.isEmpty) then reject(JwtValidationError.Malformed)
    val header = objectFields(StrictJson.parse(decodeSegment(segments(0), 8 * 1024), 8 * 1024))
    val claims = objectFields(StrictJson.parse(decodeSegment(segments(1), config.maximumTokenBytes), config.maximumTokenBytes))
    val algorithmName = string(header, "alg").getOrElse(reject(JwtValidationError.Malformed))
    val algorithm = JwtAlgorithm.Supported.find(_.jwtName == algorithmName)
      .filter(config.allowedAlgorithms.contains)
      .getOrElse(reject(JwtValidationError.UnsupportedAlgorithm))
    val keyId = string(header, "kid").filter(value => value.nonEmpty && value.length <= JwtKeySet.MaximumKeyIdChars)
      .getOrElse(reject(JwtValidationError.Malformed))
    header.get("typ").foreach {
      case JsonValue.StringValue(value) if value.equalsIgnoreCase("JWT") || value.equalsIgnoreCase("at+jwt") => ()
      case _ => reject(JwtValidationError.Malformed)
    }
    val key = keys.resolve(keyId, algorithm).getOrElse(reject(JwtValidationError.UnknownKey))
    val signatureBytes = decodeSegment(segments(2), 2048)
    if signatureBytes.isEmpty then reject(JwtValidationError.Malformed)
    val verificationSignature = if algorithm.ellipticCurve then ecdsaJoseToDer(signatureBytes, algorithm) else signatureBytes
    val verifier = Signature.getInstance(algorithm.signatureName)
    verifier.initVerify(key)
    verifier.update(s"${segments(0)}.${segments(1)}".getBytes(StandardCharsets.US_ASCII))
    if !verifier.verify(verificationSignature) then reject(JwtValidationError.InvalidSignature)

    if string(claims, "iss") != config.issuer then reject(JwtValidationError.InvalidIssuer)
    val expectedAudience = config.audience.getOrElse(reject(JwtValidationError.InvalidAudience))
    if !audiences(claims).contains(expectedAudience) then reject(JwtValidationError.InvalidAudience)
    val now = clock.instant().getEpochSecond
    val skew = config.clockSkewSeconds
    val expiration = integer(claims, "exp").getOrElse(reject(JwtValidationError.Malformed))
    if expiration <= now - skew then reject(JwtValidationError.Expired)
    integer(claims, "nbf").foreach { value =>
      if value > now + skew then reject(JwtValidationError.NotYetValid)
    }
    integer(claims, "iat").foreach { value =>
      if value > now + skew then reject(JwtValidationError.NotYetValid)
    }
    val principal = string(claims, config.principalClaim).filter(validPrincipal)
      .getOrElse(reject(JwtValidationError.InvalidPrincipal))
    val tokenScopes = scopes(claims)
    if !config.requiredScopes.subsetOf(tokenScopes) then reject(JwtValidationError.MissingScope)
    val tokenRoles = roles(claims)
    val expirationMillis =
      try Math.multiplyExact(Math.addExact(expiration, skew), 1000L)
      catch case _: ArithmeticException => reject(JwtValidationError.Malformed)
    OAuthIdentity(principal, expirationMillis, tokenScopes, tokenRoles)

  private def decodeSegment(value: String, maximumBytes: Int): Array[Byte] =
    if value.isEmpty || value.length > maximumBytes * 2 || value.exists(character => !isBase64Url(character)) then
      throw IllegalArgumentException("JWT segment is invalid")
    val bytes = Base64.getUrlDecoder.decode(value)
    if bytes.isEmpty || bytes.length > maximumBytes then throw IllegalArgumentException("JWT segment size is outside policy")
    bytes

  private def ecdsaJoseToDer(signature: Array[Byte], algorithm: JwtAlgorithm): Array[Byte] =
    val coordinateBytes = algorithm match
      case JwtAlgorithm.Es256 => 32
      case JwtAlgorithm.Es384 => 48
      case JwtAlgorithm.Es512 => 66
      case _                  => reject(JwtValidationError.Malformed)
    if signature.length != coordinateBytes * 2 then reject(JwtValidationError.Malformed)
    val first = derInteger(signature.take(coordinateBytes))
    val second = derInteger(signature.drop(coordinateBytes))
    val payload = first ++ second
    Array[Byte](0x30) ++ derLength(payload.length) ++ payload

  private def derInteger(unsigned: Array[Byte]): Array[Byte] =
    val stripped = unsigned.dropWhile(_ == 0.toByte) match
      case value if value.isEmpty => Array[Byte](0)
      case value if (value.head & 0x80) != 0 => Array[Byte](0) ++ value
      case value => value
    Array[Byte](0x02, stripped.length.toByte) ++ stripped

  private def derLength(length: Int): Array[Byte] =
    if length < 128 then Array(length.toByte)
    else Array[Byte](0x81.toByte, length.toByte)

  private def objectFields(value: JsonValue): Map[String, JsonValue] = value match
    case JsonValue.ObjectValue(fields) => fields
    case _                             => throw IllegalArgumentException("JWT JSON value must be an object")

  private def string(fields: Map[String, JsonValue], name: String): Option[String] = fields.get(name) match
    case Some(JsonValue.StringValue(value)) => Some(value)
    case None                               => None
    case _                                  => throw IllegalArgumentException(s"JWT claim $name must be a string")

  private def integer(fields: Map[String, JsonValue], name: String): Option[Long] = fields.get(name) match
    case None => None
    case Some(JsonValue.NumberValue(value)) if value.isWhole && value.isValidLong => Some(value.toLong)
    case _ => throw IllegalArgumentException(s"JWT claim $name must be an integer")

  private def audiences(fields: Map[String, JsonValue]): Set[String] = fields.get("aud") match
    case Some(JsonValue.StringValue(value)) => Set(value)
    case Some(JsonValue.ArrayValue(values)) if values.nonEmpty && values.size <= 32 =>
      values.map {
        case JsonValue.StringValue(value) if value.nonEmpty => value
        case _ => throw IllegalArgumentException("JWT audience is invalid")
      }.toSet
    case _ => throw IllegalArgumentException("JWT audience is missing or invalid")

  private def scopes(fields: Map[String, JsonValue]): Set[String] = fields.get(config.scopeClaim) match
    case None => Set.empty
    case Some(JsonValue.StringValue(value)) =>
      value.split(" ", -1).iterator.filter(_.nonEmpty).map(validateScope).toSet
    case Some(JsonValue.ArrayValue(values)) if values.size <= 256 =>
      values.map {
        case JsonValue.StringValue(value) => validateScope(value)
        case _                            => throw IllegalArgumentException("JWT scope is invalid")
      }.toSet
    case _ => throw IllegalArgumentException("JWT scope is invalid")

  private def roles(fields: Map[String, JsonValue]): Set[String] = config.roleClaim match
    case None => Set.empty
    case Some(claim) =>
      val claimed = fields.get(claim) match
        case None => Vector.empty
        case Some(JsonValue.StringValue(value)) => Vector(value)
        case Some(JsonValue.ArrayValue(values)) if values.size <= 256 => values.map {
          case JsonValue.StringValue(value) => value
          case _ => throw IllegalArgumentException("JWT role claim is invalid")
        }
        case _ => throw IllegalArgumentException("JWT role claim is invalid")
      claimed.iterator.map(validateRoleClaim).flatMap(config.roleMappings.get).toSet

  private def validateRoleClaim(value: String): String =
    if value.isEmpty || value.length > 256 || value.exists(_.isWhitespace) then
      throw IllegalArgumentException("JWT role claim is invalid")
    value

  private def validateScope(value: String): String =
    if value.isEmpty || value.length > 256 || value.exists(_.isWhitespace) then
      throw IllegalArgumentException("JWT scope is invalid")
    value

  private def validPrincipal(value: String): Boolean =
    value.nonEmpty && value.length <= 255 && !value.exists(character => character.isControl || character.isWhitespace)

  private def isBase64Url(character: Char): Boolean =
    character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' ||
      character >= '0' && character <= '9' || character == '-' || character == '_'

  private def reject(error: JwtValidationError): Nothing = throw JwtRejected(error)

final case class OAuthBearerRequest(token: String, authorizationId: Option[String])

final class OAuthBearerAuthenticator(config: OAuthConfig, validator: JwtValidator):
  def authenticate(bytes: Array[Byte]): Option[OAuthIdentity] =
    try
      val request = OAuthBearerMessage.parse(bytes, config.maximumTokenBytes)
      validator.validate(request.token).toOption.filter(identity => request.authorizationId.forall(_ == identity.principal))
    catch case NonFatal(_) => None

object OAuthBearerMessage:
  val MaximumAttributes = 16
  val MaximumEnvelopeBytes = 4096

  def parse(bytes: Array[Byte], maximumTokenBytes: Int): OAuthBearerRequest =
    if bytes.isEmpty || bytes.length > maximumTokenBytes + MaximumEnvelopeBytes then
      invalid("OAUTHBEARER message size is outside policy")
    val message = decodeUtf8(bytes)
    val separator = message.indexOf('\u0001')
    if separator < 0 then invalid("OAUTHBEARER message has no GS2 separator")
    val authorizationId = parseGs2Header(message.substring(0, separator))
    val parts = message.substring(separator + 1).split("\u0001", -1).toVector
    if parts.length < 3 || parts.takeRight(2) != Vector("", "") then
      invalid("OAUTHBEARER message is not terminated")
    val attributes = parts.dropRight(2)
    if attributes.isEmpty || attributes.length > MaximumAttributes then
      invalid("OAUTHBEARER attribute count is outside policy")
    val values = attributes.foldLeft(Map.empty[String, String]) { (current, attribute) =>
      val equals = attribute.indexOf('=')
      if equals <= 0 then invalid("OAUTHBEARER attribute is malformed")
      val key = attribute.substring(0, equals)
      if !key.forall(character => character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z') || current.contains(key) then
        invalid("OAUTHBEARER attribute is invalid or duplicated")
      current.updated(key, attribute.substring(equals + 1))
    }
    val authorization = values.getOrElse("auth", invalid("OAUTHBEARER authorization value is missing"))
    val space = authorization.indexOf(' ')
    if space <= 0 || !authorization.substring(0, space).equalsIgnoreCase("Bearer") then
      invalid("OAUTHBEARER authorization scheme is invalid")
    val token = authorization.substring(space + 1)
    if token.isEmpty || token.length > maximumTokenBytes || token.exists(character => character > 0x7f || character.isWhitespace) then
      invalid("OAUTHBEARER token is outside policy")
    OAuthBearerRequest(token, authorizationId)

  private def parseGs2Header(value: String): Option[String] =
    if !value.startsWith("n,") || !value.endsWith(",") then
      invalid("OAUTHBEARER GS2 header is invalid")
    val identity = value.substring(2, value.length - 1)
    if identity.isEmpty then None
    else if identity.startsWith("a=") then Some(decodeName(identity.substring(2)))
    else invalid("OAUTHBEARER GS2 authorization identity is invalid")

  private def decodeName(value: String): String =
    val result = StringBuilder(value.length)
    var index = 0
    while index < value.length do
      if value.charAt(index) == '=' then
        if index + 2 >= value.length then invalid("OAUTHBEARER authorization identity escape is incomplete")
        value.substring(index, index + 3) match
          case "=2C" => result.append(',')
          case "=3D" => result.append('=')
          case _     => invalid("OAUTHBEARER authorization identity escape is invalid")
        index += 3
      else
        result.append(value.charAt(index))
        index += 1
    val decoded = result.result()
    if decoded.isEmpty || decoded.length > 255 || decoded.exists(character => character.isControl || character.isWhitespace) then
      invalid("OAUTHBEARER authorization identity is outside policy")
    decoded

  private def decodeUtf8(bytes: Array[Byte]): String =
    try
      StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString
    catch case _: java.nio.charset.CharacterCodingException => invalid("OAUTHBEARER message is not valid UTF-8")

  private def invalid(message: String): Nothing = throw IllegalArgumentException(message)

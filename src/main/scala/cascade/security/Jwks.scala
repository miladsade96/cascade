package cascade.security

import java.io.InputStream
import java.math.BigInteger
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.{Files, Path}
import java.security.{AlgorithmParameters, KeyFactory, PublicKey}
import java.security.spec.{ECGenParameterSpec, ECParameterSpec, ECPoint, ECPublicKeySpec, RSAPublicKeySpec, X509EncodedKeySpec}
import java.time.Duration
import java.util.Base64
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicReference}
import scala.util.control.NonFatal

private[security] final case class JwtVerificationKey(
    keyId: String,
    algorithms: Set[JwtAlgorithm],
    publicKey: PublicKey
)

private[security] final class JwtKeySet private (keys: Map[String, JwtVerificationKey]):
  def resolve(keyId: String, algorithm: JwtAlgorithm): Option[PublicKey] =
    keys.get(keyId).filter(_.algorithms.contains(algorithm)).map(_.publicKey)

  def keyIds: Set[String] = keys.keySet

private[security] object JwtKeySet:
  val MaximumDocumentBytes = 1024 * 1024
  val MaximumKeys = 128
  val MaximumKeyIdChars = 256
  val MinimumRsaBits = 2048
  val MaximumRsaBits = 8192

  def parse(bytes: Array[Byte]): JwtKeySet =
    val root = objectFields(StrictJson.parse(bytes, MaximumDocumentBytes), "JWKS root")
    val values = root.get("keys") match
      case Some(JsonValue.ArrayValue(entries)) => entries
      case _                                   => invalid("JWKS keys array is missing")
    if values.isEmpty || values.size > MaximumKeys then invalid("JWKS key count is outside policy")
    val parsed = values.flatMap(parseKey)
    if parsed.isEmpty then invalid("JWKS contains no usable signing keys")
    val duplicates = parsed.groupBy(_.keyId).collect { case (keyId, entries) if entries.size > 1 => keyId }
    if duplicates.nonEmpty then invalid("JWKS contains duplicate key IDs")
    new JwtKeySet(parsed.map(key => key.keyId -> key).toMap)

  private def parseKey(value: JsonValue): Option[JwtVerificationKey] =
    val fields = objectFields(value, "JWK")
    string(fields, "kty") match
      case Some("RSA") => parseRsa(fields)
      case Some("EC")  => parseEc(fields)
      case Some("OKP") => parseOkp(fields)
      case _           => None

  private def parseRsa(fields: Map[String, JsonValue]): Option[JwtVerificationKey] =
    metadata(fields, "RSA") match
      case None => None
      case Some((_, algorithm)) if algorithm.exists(!_.rsa) => None
      case Some((keyId, algorithm)) =>
        val algorithms = algorithm.map(Set(_)).getOrElse(JwtAlgorithm.Supported.filter(_.rsa).toSet)
        val modulus = unsignedInteger(string(fields, "n").getOrElse(invalid("RSA modulus is missing")))
        val exponent = unsignedInteger(string(fields, "e").getOrElse(invalid("RSA exponent is missing")))
        if modulus.bitLength < MinimumRsaBits || modulus.bitLength > MaximumRsaBits then
          invalid("RSA modulus size is outside policy")
        if exponent.compareTo(BigInteger.valueOf(3L)) < 0 || !exponent.testBit(0) || exponent.bitLength > 32 then
          invalid("RSA exponent is outside policy")
        val publicKey =
          try KeyFactory.getInstance("RSA").generatePublic(RSAPublicKeySpec(modulus, exponent))
          catch case error: java.security.GeneralSecurityException => throw IllegalArgumentException("RSA JWK is invalid", error)
        Some(JwtVerificationKey(keyId, algorithms, publicKey))

  private def parseEc(fields: Map[String, JsonValue]): Option[JwtVerificationKey] =
    val curve = string(fields, "crv") match
      case Some("P-256") => Some(("secp256r1", JwtAlgorithm.Es256, 32))
      case Some("P-384") => Some(("secp384r1", JwtAlgorithm.Es384, 48))
      case Some("P-521") => Some(("secp521r1", JwtAlgorithm.Es512, 66))
      case _             => None
    (metadata(fields, "EC"), curve) match
      case (Some((keyId, algorithm)), Some((curveName, expectedAlgorithm, coordinateBytes)))
          if algorithm.forall(_ == expectedAlgorithm) =>
        val x = fixedUnsigned(string(fields, "x").getOrElse(invalid("EC x coordinate is missing")), coordinateBytes)
        val y = fixedUnsigned(string(fields, "y").getOrElse(invalid("EC y coordinate is missing")), coordinateBytes)
        val publicKey =
          try
            val parameters = AlgorithmParameters.getInstance("EC")
            parameters.init(ECGenParameterSpec(curveName))
            val specification = parameters.getParameterSpec(classOf[ECParameterSpec])
            KeyFactory.getInstance("EC").generatePublic(ECPublicKeySpec(ECPoint(BigInteger(1, x), BigInteger(1, y)), specification))
          catch case error: java.security.GeneralSecurityException => throw IllegalArgumentException("EC JWK is invalid", error)
        Some(JwtVerificationKey(keyId, Set(expectedAlgorithm), publicKey))
      case _ => None

  private def parseOkp(fields: Map[String, JsonValue]): Option[JwtVerificationKey] =
    metadata(fields, "OKP") match
      case Some((keyId, algorithm)) if string(fields, "crv") == Some("Ed25519") && algorithm.forall(_ == JwtAlgorithm.EdDsa) =>
        val x = fixedUnsigned(string(fields, "x").getOrElse(invalid("OKP public key is missing")), 32)
        val prefix = Array[Byte](0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00)
        val publicKey =
          try KeyFactory.getInstance("Ed25519").generatePublic(X509EncodedKeySpec(prefix ++ x))
          catch case error: java.security.GeneralSecurityException => throw IllegalArgumentException("OKP JWK is invalid", error)
        Some(JwtVerificationKey(keyId, Set(JwtAlgorithm.EdDsa), publicKey))
      case _ => None

  private def metadata(fields: Map[String, JsonValue], keyType: String): Option[(String, Option[JwtAlgorithm])] =
    val keyId = string(fields, "kid").getOrElse(invalid(s"$keyType JWK key ID is missing"))
    if keyId.isEmpty || keyId.length > MaximumKeyIdChars || keyId.exists(_.isControl) then
      invalid(s"$keyType JWK key ID is outside policy")
    val use = string(fields, "use")
    val operations = fields.get("key_ops") match
      case None => None
      case Some(JsonValue.ArrayValue(values)) =>
        Some(values.map {
          case JsonValue.StringValue(operation) => operation
          case _                                => invalid("JWK key operations are invalid")
        }.toSet)
      case _ => invalid("JWK key operations are invalid")
    if use.exists(_ != "sig") || operations.exists(!_.contains("verify")) then return None
    val algorithm = string(fields, "alg") match
      case None => None
      case Some(name) => JwtAlgorithm.Supported.find(_.jwtName == name) match
        case Some(value) => Some(value)
        case None        => return None
    Some(keyId -> algorithm)

  private def unsignedInteger(value: String): BigInteger =
    if value.isEmpty || value.length > 2048 || value.exists(character => !isBase64Url(character)) then
      invalid("JWK integer is invalid")
    val bytes =
      try Base64.getUrlDecoder.decode(value)
      catch case _: IllegalArgumentException => invalid("JWK integer is invalid")
    if bytes.isEmpty then invalid("JWK integer is empty")
    BigInteger(1, bytes)

  private def fixedUnsigned(value: String, expectedBytes: Int): Array[Byte] =
    if value.isEmpty || value.exists(character => !isBase64Url(character)) then invalid("JWK coordinate is invalid")
    val bytes =
      try Base64.getUrlDecoder.decode(value)
      catch case _: IllegalArgumentException => invalid("JWK coordinate is invalid")
    if bytes.length != expectedBytes then invalid("JWK coordinate size is outside policy")
    bytes

  private def objectFields(value: JsonValue, name: String): Map[String, JsonValue] = value match
    case JsonValue.ObjectValue(fields) => fields
    case _                             => invalid(s"$name must be an object")

  private def string(fields: Map[String, JsonValue], name: String): Option[String] = fields.get(name) match
    case None                          => None
    case Some(JsonValue.StringValue(value)) => Some(value)
    case _                             => invalid(s"JWK $name must be a string")

  private def isBase64Url(character: Char): Boolean =
    character >= 'A' && character <= 'Z' || character >= 'a' && character <= 'z' ||
      character >= '0' && character <= '9' || character == '-' || character == '_'

  private def invalid(message: String): Nothing = throw IllegalArgumentException(message)

private[security] sealed trait JwksFetch
private[security] final case class JwksLoaded(bytes: Array[Byte], entityTag: Option[String]) extends JwksFetch
private[security] case object JwksNotModified extends JwksFetch

private[security] final class JwksSource(uri: URI, timeoutMillis: Int, client: Option[HttpClient] = None):
  private val http = client.getOrElse {
    HttpClient.newBuilder()
      .connectTimeout(Duration.ofMillis(timeoutMillis.toLong))
      .followRedirects(HttpClient.Redirect.NEVER)
      .build()
  }

  def fetch(entityTag: Option[String]): JwksFetch = Option(uri.getScheme).map(_.toLowerCase) match
    case Some("file") =>
      val path = Path.of(uri)
      val size = Files.size(path)
      if size <= 0L || size > JwtKeySet.MaximumDocumentBytes then
        throw IllegalArgumentException("JWKS file size is outside policy")
      JwksLoaded(Files.readAllBytes(path), None)
    case Some("https") => fetchHttps(entityTag)
    case _             => throw IllegalArgumentException("JWKS URI must use file or https")

  private def fetchHttps(entityTag: Option[String]): JwksFetch =
    val builder = HttpRequest.newBuilder(uri)
      .timeout(Duration.ofMillis(timeoutMillis.toLong))
      .header("Accept", "application/json")
      .GET()
    entityTag.foreach(value => builder.header("If-None-Match", value): Unit)
    val response =
      try http.send(builder.build(), HttpResponse.BodyHandlers.ofInputStream())
      catch
        case error: InterruptedException =>
          Thread.currentThread().interrupt()
          throw IllegalStateException("JWKS HTTPS request was interrupted", error)
    val body = response.body()
    try
      response.statusCode() match
        case 304 => JwksNotModified
        case 200 =>
          val contentLength = response.headers().firstValueAsLong("Content-Length")
          if contentLength.isPresent then
            val length = contentLength.getAsLong
            if length <= 0L || length > JwtKeySet.MaximumDocumentBytes then
              throw IllegalArgumentException("JWKS HTTPS body size is outside policy")
          val bytes = readBounded(body)
          val header = response.headers().firstValue("ETag")
          val tag = Option.when(header.isPresent)(header.get()).map { value =>
            if value.isEmpty || value.length > 512 then throw IllegalArgumentException("JWKS ETag is outside policy")
            value
          }
          JwksLoaded(bytes, tag)
        case status => throw IllegalArgumentException(s"JWKS HTTPS endpoint returned status $status")
    finally body.close()

  private def readBounded(input: InputStream): Array[Byte] =
    val bytes = input.readNBytes(JwtKeySet.MaximumDocumentBytes + 1)
    if bytes.isEmpty || bytes.length > JwtKeySet.MaximumDocumentBytes then
      throw IllegalArgumentException("JWKS HTTPS body size is outside policy")
    bytes

final class ReloadableJwks(config: OAuthConfig) extends AutoCloseable:
  private val source = JwksSource(config.jwksUri.getOrElse(throw IllegalArgumentException("JWKS URI is missing")), config.httpTimeoutMillis)
  private val entityTag = AtomicReference(Option.empty[String])
  private val keys = AtomicReference(loadInitial())
  private val reloadError = AtomicReference(Option.empty[String])
  private val scheduler: Option[ScheduledExecutorService] = Option.when(config.jwksRefreshMillis > 0L) {
    val executor = Executors.newSingleThreadScheduledExecutor { runnable =>
      Thread.ofPlatform().daemon(true).name("cascade-jwks-refresh").unstarted(runnable)
    }
    executor.scheduleWithFixedDelay(
      () => reloadNow(): Unit,
      config.jwksRefreshMillis,
      config.jwksRefreshMillis,
      TimeUnit.MILLISECONDS
    )
    executor
  }

  def resolve(keyId: String, algorithm: JwtAlgorithm): Option[PublicKey] =
    if config.jwksRefreshMillis == 0L then reloadNow(): Unit
    keys.get().resolve(keyId, algorithm)

  def keyIds: Set[String] = keys.get().keyIds

  def lastReloadError: Option[String] =
    if config.jwksRefreshMillis == 0L then reloadNow(): Unit
    reloadError.get()

  def reloadNow(): Boolean = synchronized {
    try
      source.fetch(entityTag.get()) match
        case JwksNotModified => ()
        case JwksLoaded(bytes, tag) =>
          val replacement = JwtKeySet.parse(bytes)
          keys.set(replacement)
          entityTag.set(tag)
      reloadError.set(None)
      true
    catch
      case NonFatal(error) =>
        reloadError.set(Some(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))
        false
  }

  override def close(): Unit = scheduler.foreach(_.shutdownNow(): Unit)

  private def loadInitial(): JwtKeySet = source.fetch(None) match
    case JwksLoaded(bytes, tag) =>
      val initial = JwtKeySet.parse(bytes)
      entityTag.set(tag)
      initial
    case JwksNotModified => throw IllegalStateException("initial JWKS request returned not modified")

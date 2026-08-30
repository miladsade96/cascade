package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.{KeyPair, KeyPairGenerator, PrivateKey, Signature}
import java.security.interfaces.RSAPublicKey
import java.util.Base64

object OAuthTestSupport:
  def keyPair(bits: Int = 2048): KeyPair =
    val generator = KeyPairGenerator.getInstance("RSA")
    generator.initialize(bits)
    generator.generateKeyPair()

  def writeJwks(path: Path, entries: Vector[(String, KeyPair)], algorithm: Option[String] = Some("RS256")): Unit =
    Files.writeString(path, jwks(entries, algorithm), StandardCharsets.UTF_8): Unit

  def jwks(entries: Vector[(String, KeyPair)], algorithm: Option[String] = Some("RS256")): String =
    val keys = entries.map { case (keyId, pair) =>
      val key = pair.getPublic.asInstanceOf[RSAPublicKey]
      val algorithmField = algorithm.map(value => s",\"alg\":\"${escape(value)}\"").getOrElse("")
      s"{\"kty\":\"RSA\",\"kid\":\"${escape(keyId)}\",\"use\":\"sig\"$algorithmField,\"n\":\"${unsigned(key.getModulus.toByteArray)}\",\"e\":\"${unsigned(key.getPublicExponent.toByteArray)}\"}"
    }
    s"{\"keys\":[${keys.mkString(",")}] }"

  def token(
      privateKey: PrivateKey,
      keyId: String,
      claimsJson: String,
      algorithm: JwtAlgorithm = JwtAlgorithm.Rs256,
      tokenAlgorithm: Option[String] = None
  ): String =
    val header = s"{\"alg\":\"${tokenAlgorithm.getOrElse(algorithm.jwtName)}\",\"kid\":\"${escape(keyId)}\",\"typ\":\"JWT\"}"
    sign(privateKey, header, claimsJson, algorithm)

  def sign(privateKey: PrivateKey, headerJson: String, claimsJson: String, algorithm: JwtAlgorithm): String =
    val header = base64(headerJson.getBytes(StandardCharsets.UTF_8))
    val claims = base64(claimsJson.getBytes(StandardCharsets.UTF_8))
    val input = s"$header.$claims"
    val signer = Signature.getInstance(algorithm.signatureName)
    signer.initSign(privateKey)
    signer.update(input.getBytes(StandardCharsets.US_ASCII))
    s"$input.${base64(signer.sign())}"

  def claims(
      issuer: String,
      audienceJson: String,
      subject: String,
      issuedAt: Long,
      expiresAt: Long,
      scopesJson: String = "\"cascade.read cascade.write\"",
      extra: String = ""
  ): String =
    val suffix = if extra.isEmpty then "" else s",$extra"
    s"{\"iss\":\"${escape(issuer)}\",\"aud\":$audienceJson,\"sub\":\"${escape(subject)}\",\"iat\":$issuedAt,\"exp\":$expiresAt,\"scope\":$scopesJson$suffix}"

  private def unsigned(bytes: Array[Byte]): String =
    val first = bytes.indexWhere(_ != 0.toByte)
    val normalized = if first < 0 then Array(0.toByte) else bytes.drop(first)
    base64(normalized)

  private def base64(bytes: Array[Byte]): String = Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)

  private def escape(value: String): String = value.flatMap {
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case character => character.toString
  }

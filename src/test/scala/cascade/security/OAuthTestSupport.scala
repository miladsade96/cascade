package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.{KeyPair, KeyPairGenerator, PrivateKey, Signature}
import java.security.interfaces.{ECPublicKey, RSAPublicKey}
import java.security.spec.ECGenParameterSpec
import java.util.Base64

object OAuthTestSupport:
  def keyPair(bits: Int = 2048): KeyPair =
    val generator = KeyPairGenerator.getInstance("RSA")
    generator.initialize(bits)
    generator.generateKeyPair()

  def keyPairFor(algorithm: JwtAlgorithm): KeyPair =
    if algorithm.rsa then keyPair()
    else if algorithm.ellipticCurve then
      val generator = KeyPairGenerator.getInstance("EC")
      val curve = algorithm match
        case JwtAlgorithm.Es256 => "secp256r1"
        case JwtAlgorithm.Es384 => "secp384r1"
        case JwtAlgorithm.Es512 => "secp521r1"
        case _                  => throw IllegalArgumentException("not an EC algorithm")
      generator.initialize(ECGenParameterSpec(curve))
      generator.generateKeyPair()
    else KeyPairGenerator.getInstance("Ed25519").generateKeyPair()

  def writeJwks(path: Path, entries: Vector[(String, KeyPair)], algorithm: Option[String] = Some("RS256")): Unit =
    Files.writeString(path, jwks(entries, algorithm), StandardCharsets.UTF_8): Unit

  def jwks(entries: Vector[(String, KeyPair)], algorithm: Option[String] = Some("RS256")): String =
    val keys = entries.map { case (keyId, pair) =>
      val algorithmField = algorithm.map(value => s",\"alg\":\"${escape(value)}\"").getOrElse("")
      pair.getPublic match
        case key: RSAPublicKey =>
          s"{\"kty\":\"RSA\",\"kid\":\"${escape(keyId)}\",\"use\":\"sig\"$algorithmField,\"n\":\"${unsigned(key.getModulus.toByteArray)}\",\"e\":\"${unsigned(key.getPublicExponent.toByteArray)}\"}"
        case key: ECPublicKey =>
          val bits = key.getParams.getCurve.getField.getFieldSize
          val (curve, bytes) = bits match
            case 256 => "P-256" -> 32
            case 384 => "P-384" -> 48
            case 521 => "P-521" -> 66
            case _   => throw IllegalArgumentException(s"unsupported test curve: $bits")
          s"{\"kty\":\"EC\",\"kid\":\"${escape(keyId)}\",\"use\":\"sig\"$algorithmField,\"crv\":\"$curve\",\"x\":\"${fixed(key.getW.getAffineX.toByteArray, bytes)}\",\"y\":\"${fixed(key.getW.getAffineY.toByteArray, bytes)}\"}"
        case key if key.getAlgorithm.equalsIgnoreCase("EdDSA") || key.getAlgorithm.equalsIgnoreCase("Ed25519") =>
          s"{\"kty\":\"OKP\",\"kid\":\"${escape(keyId)}\",\"use\":\"sig\"$algorithmField,\"crv\":\"Ed25519\",\"x\":\"${base64(key.getEncoded.takeRight(32))}\"}"
        case key => throw IllegalArgumentException(s"unsupported test public key: ${key.getAlgorithm}")
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
    val signature = signer.sign()
    val joseSignature = if algorithm.ellipticCurve then ecdsaDerToJose(signature, algorithm) else signature
    s"$input.${base64(joseSignature)}"

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

  private def fixed(bytes: Array[Byte], size: Int): String =
    val unsigned = bytes.dropWhile(_ == 0.toByte)
    if unsigned.length > size then throw IllegalArgumentException("test coordinate is too large")
    base64(Array.fill[Byte](size - unsigned.length)(0) ++ unsigned)

  private def ecdsaDerToJose(der: Array[Byte], algorithm: JwtAlgorithm): Array[Byte] =
    val coordinateBytes = algorithm match
      case JwtAlgorithm.Es256 => 32
      case JwtAlgorithm.Es384 => 48
      case JwtAlgorithm.Es512 => 66
      case _                  => throw IllegalArgumentException("not an EC algorithm")
    var offset = 0
    if der(offset) != 0x30.toByte then throw IllegalArgumentException("invalid DER sequence")
    offset += 1
    val sequenceLength = der(offset) & 0xff
    offset += (if sequenceLength == 0x81 then 2 else 1)
    def integer(): Array[Byte] =
      if der(offset) != 0x02.toByte then throw IllegalArgumentException("invalid DER integer")
      val length = der(offset + 1) & 0xff
      val value = der.slice(offset + 2, offset + 2 + length).dropWhile(_ == 0.toByte)
      offset += 2 + length
      if value.length > coordinateBytes then throw IllegalArgumentException("DER coordinate is too large")
      Array.fill[Byte](coordinateBytes - value.length)(0) ++ value
    integer() ++ integer()

  private def base64(bytes: Array[Byte]): String = Base64.getUrlEncoder.withoutPadding().encodeToString(bytes)

  private def escape(value: String): String = value.flatMap {
    case '"'  => "\\\""
    case '\\' => "\\\\"
    case character => character.toString
  }

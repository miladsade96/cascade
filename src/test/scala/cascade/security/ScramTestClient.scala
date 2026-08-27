package cascade.security

import java.nio.charset.StandardCharsets
import java.util.{Arrays, Base64}
import javax.crypto.{Mac, SecretKeyFactory}
import javax.crypto.spec.{PBEKeySpec, SecretKeySpec}

final case class ScramTestResponse(clientFinal: String, expectedServerFinal: String)

object ScramTestClient:
  def respond(
      mechanism: SaslMechanism,
      password: Array[Char],
      clientFirstBare: String,
      serverFirst: String
  ): ScramTestResponse =
    val attributes = serverFirst.split(",").map { part => part.charAt(0) -> part.substring(2) }.toMap
    val salt = Base64.getDecoder.decode(attributes('s'))
    val iterations = attributes('i').toInt
    val clientFinalWithoutProof = s"c=biws,r=${attributes('r')}"
    val authMessage = s"$clientFirstBare,$serverFirst,$clientFinalWithoutProof"
    val specification = PBEKeySpec(password, salt, iterations, keyBytes(mechanism) * 8)
    val salted =
      try SecretKeyFactory.getInstance(pbkdf2(mechanism)).generateSecret(specification).getEncoded
      finally specification.clearPassword()
    val clientKey = hmac(mechanism, salted, "Client Key")
    val storedKey = java.security.MessageDigest.getInstance(digest(mechanism)).digest(clientKey)
    val clientSignature = hmac(mechanism, storedKey, authMessage)
    val proof = clientKey.indices.map(index => (clientKey(index) ^ clientSignature(index)).toByte).toArray
    val serverKey = hmac(mechanism, salted, "Server Key")
    val serverSignature = hmac(mechanism, serverKey, authMessage)
    try
      ScramTestResponse(
        s"$clientFinalWithoutProof,p=${Base64.getEncoder.encodeToString(proof)}",
        s"v=${Base64.getEncoder.encodeToString(serverSignature)}"
      )
    finally
      Vector(salt, salted, clientKey, storedKey, clientSignature, proof, serverKey, serverSignature).foreach(Arrays.fill(_, 0.toByte))

  private def hmac(mechanism: SaslMechanism, key: Array[Byte], value: String): Array[Byte] =
    val algorithm = if mechanism == SaslMechanism.ScramSha256 then "HmacSHA256" else "HmacSHA512"
    val mac = Mac.getInstance(algorithm)
    mac.init(SecretKeySpec(key, algorithm))
    mac.doFinal(value.getBytes(StandardCharsets.UTF_8))

  private def pbkdf2(mechanism: SaslMechanism): String =
    if mechanism == SaslMechanism.ScramSha256 then "PBKDF2WithHmacSHA256" else "PBKDF2WithHmacSHA512"

  private def digest(mechanism: SaslMechanism): String =
    if mechanism == SaslMechanism.ScramSha256 then "SHA-256" else "SHA-512"

  private def keyBytes(mechanism: SaslMechanism): Int =
    if mechanism == SaslMechanism.ScramSha256 then 32 else 64

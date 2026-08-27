package cascade.security

import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, SecureRandom}
import java.util.Arrays
import javax.crypto.{Mac, SecretKeyFactory}
import javax.crypto.spec.{PBEKeySpec, SecretKeySpec}

final class ScramCredential private (
    val mechanism: SaslMechanism,
    val iterations: Int,
    saltBytes: Array[Byte],
    storedKeyBytes: Array[Byte],
    serverKeyBytes: Array[Byte]
):
  require(mechanism.scram, "SCRAM credentials require a SCRAM mechanism")
  require(
    iterations >= ScramCredential.MinimumIterations && iterations <= ScramCredential.MaximumIterations,
    s"SCRAM iterations must be between ${ScramCredential.MinimumIterations} and ${ScramCredential.MaximumIterations}"
  )
  require(saltBytes.length >= ScramCredential.MinimumSaltBytes, "SCRAM salt is too short")
  require(storedKeyBytes.length == ScramCredential.keyBytes(mechanism), "SCRAM stored key has the wrong length")
  require(serverKeyBytes.length == ScramCredential.keyBytes(mechanism), "SCRAM server key has the wrong length")

  private val saltValue = saltBytes.clone()
  private val storedKeyValue = storedKeyBytes.clone()
  private val serverKeyValue = serverKeyBytes.clone()

  def salt: Array[Byte] = saltValue.clone()

  private[security] def storedKey: Array[Byte] = storedKeyValue.clone()

  private[security] def serverKey: Array[Byte] = serverKeyValue.clone()

  def authenticate(authMessage: String, clientProof: Array[Byte]): Option[Array[Byte]] =
    if clientProof.length != storedKeyValue.length then None
    else
      val message = authMessage.getBytes(StandardCharsets.UTF_8)
      val clientSignature = ScramCredential.hmac(mechanism, storedKeyValue, message)
      val recoveredClientKey = new Array[Byte](clientProof.length)
      var index = 0
      while index < clientProof.length do
        recoveredClientKey(index) = (clientProof(index) ^ clientSignature(index)).toByte
        index += 1
      val recoveredStoredKey = ScramCredential.digest(mechanism, recoveredClientKey)
      try
        Option.when(MessageDigest.isEqual(storedKeyValue, recoveredStoredKey)) {
          ScramCredential.hmac(mechanism, serverKeyValue, message)
        }
      finally
        Arrays.fill(message, 0.toByte)
        Arrays.fill(clientSignature, 0.toByte)
        Arrays.fill(recoveredClientKey, 0.toByte)
        Arrays.fill(recoveredStoredKey, 0.toByte)

object ScramCredential:
  val MinimumIterations = 4096
  val RecommendedIterations = 16384
  val MaximumIterations = 1_000_000
  val MinimumSaltBytes = 16
  private val GeneratedSaltBytes = 32
  private val random = SecureRandom()

  def create(
      mechanism: SaslMechanism,
      password: Array[Char],
      iterations: Int = RecommendedIterations
  ): ScramCredential =
    val salt = new Array[Byte](GeneratedSaltBytes)
    random.nextBytes(salt)
    fromPassword(mechanism, password, iterations, salt)

  private[security] def fromPassword(
      mechanism: SaslMechanism,
      password: Array[Char],
      iterations: Int,
      salt: Array[Byte]
  ): ScramCredential =
    require(mechanism.scram, "SCRAM credentials require a SCRAM mechanism")
    require(iterations >= MinimumIterations && iterations <= MaximumIterations, "SCRAM iteration count is outside policy")
    require(salt.length >= MinimumSaltBytes, "SCRAM salt is too short")
    val specification = PBEKeySpec(password, salt, iterations, keyBytes(mechanism) * 8)
    val saltedPassword =
      try SecretKeyFactory.getInstance(pbkdf2Algorithm(mechanism)).generateSecret(specification).getEncoded
      finally specification.clearPassword()
    val clientKey = hmac(mechanism, saltedPassword, "Client Key".getBytes(StandardCharsets.US_ASCII))
    val storedKey = digest(mechanism, clientKey)
    val serverKey = hmac(mechanism, saltedPassword, "Server Key".getBytes(StandardCharsets.US_ASCII))
    try new ScramCredential(mechanism, iterations, salt, storedKey, serverKey)
    finally
      Arrays.fill(saltedPassword, 0.toByte)
      Arrays.fill(clientKey, 0.toByte)
      Arrays.fill(storedKey, 0.toByte)
      Arrays.fill(serverKey, 0.toByte)

  private[security] def fromKeys(
      mechanism: SaslMechanism,
      iterations: Int,
      salt: Array[Byte],
      storedKey: Array[Byte],
      serverKey: Array[Byte]
  ): ScramCredential = new ScramCredential(mechanism, iterations, salt, storedKey, serverKey)

  private[security] def hmac(mechanism: SaslMechanism, key: Array[Byte], value: Array[Byte]): Array[Byte] =
    val mac = Mac.getInstance(hmacAlgorithm(mechanism))
    mac.init(SecretKeySpec(key, hmacAlgorithm(mechanism)))
    mac.doFinal(value)

  private[security] def digest(mechanism: SaslMechanism, value: Array[Byte]): Array[Byte] =
    MessageDigest.getInstance(digestAlgorithm(mechanism)).digest(value)

  private[security] def keyBytes(mechanism: SaslMechanism): Int = mechanism match
    case SaslMechanism.ScramSha256 => 32
    case SaslMechanism.ScramSha512 => 64
    case SaslMechanism.Plain       => throw IllegalArgumentException("PLAIN does not use SCRAM keys")

  private def digestAlgorithm(mechanism: SaslMechanism): String = mechanism match
    case SaslMechanism.ScramSha256 => "SHA-256"
    case SaslMechanism.ScramSha512 => "SHA-512"
    case SaslMechanism.Plain       => throw IllegalArgumentException("PLAIN does not use a SCRAM digest")

  private def hmacAlgorithm(mechanism: SaslMechanism): String = mechanism match
    case SaslMechanism.ScramSha256 => "HmacSHA256"
    case SaslMechanism.ScramSha512 => "HmacSHA512"
    case SaslMechanism.Plain       => throw IllegalArgumentException("PLAIN does not use a SCRAM HMAC")

  private def pbkdf2Algorithm(mechanism: SaslMechanism): String = mechanism match
    case SaslMechanism.ScramSha256 => "PBKDF2WithHmacSHA256"
    case SaslMechanism.ScramSha512 => "PBKDF2WithHmacSHA512"
    case SaslMechanism.Plain       => throw IllegalArgumentException("PLAIN does not use SCRAM PBKDF2")

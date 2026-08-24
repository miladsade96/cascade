package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.{MessageDigest, SecureRandom}
import java.util.{Arrays, Base64}
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec

final case class PasswordCredential(iterations: Int, salt: Array[Byte], expectedHash: Array[Byte]):
  require(iterations >= CredentialHash.MinimumIterations, s"PBKDF2 iterations must be at least ${CredentialHash.MinimumIterations}")
  require(salt.length >= 16, "credential salt must contain at least 16 bytes")
  require(expectedHash.length >= 32, "credential hash must contain at least 32 bytes")

  def verify(password: Array[Char]): Boolean =
    val actual = CredentialHash.derive(password, salt, iterations, expectedHash.length)
    try MessageDigest.isEqual(expectedHash, actual)
    finally Arrays.fill(actual, 0.toByte)

object CredentialHash:
  val Algorithm = "pbkdf2-sha256"
  val MinimumIterations = 10_000
  val RecommendedIterations = 210_000
  private val random = SecureRandom()

  def create(password: Array[Char], iterations: Int = RecommendedIterations): String =
    require(iterations >= MinimumIterations, s"PBKDF2 iterations must be at least $MinimumIterations")
    val salt = new Array[Byte](16)
    random.nextBytes(salt)
    val hash = derive(password, salt, iterations, 32)
    try
      val encoder = Base64.getEncoder
      Vector(Algorithm, iterations.toString, encoder.encodeToString(salt), encoder.encodeToString(hash)).mkString("$")
    finally Arrays.fill(hash, 0.toByte)

  private[security] def derive(password: Array[Char], salt: Array[Byte], iterations: Int, bytes: Int): Array[Byte] =
    val specification = PBEKeySpec(password, salt, iterations, bytes * 8)
    try SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(specification).getEncoded
    finally specification.clearPassword()

object CredentialFile:
  def load(path: Path): Map[String, PasswordCredential] =
    val entries = Files.readAllLines(path, StandardCharsets.UTF_8)
    import scala.jdk.CollectionConverters.*
    entries.asScala.iterator.zipWithIndex.foldLeft(Map.empty[String, PasswordCredential]) { case (credentials, (raw, index)) =>
      val line = raw.trim
      if line.isEmpty || line.startsWith("#") then credentials
      else
        val separator = line.indexOf('=')
        if separator <= 0 then throw IllegalArgumentException(s"invalid credential at ${path.getFileName}:${index + 1}")
        val user = line.substring(0, separator).trim
        if credentials.contains(user) then throw IllegalArgumentException(s"duplicate credential for '$user'")
        credentials.updated(user, parseCredential(line.substring(separator + 1).trim, path, index + 1))
    }

  private def parseCredential(value: String, path: Path, line: Int): PasswordCredential =
    val fields = value.split("\\$", -1)
    if fields.length != 4 || fields(0) != CredentialHash.Algorithm then
      throw IllegalArgumentException(s"invalid credential hash at ${path.getFileName}:$line")
    try
      PasswordCredential(
        fields(1).toInt,
        Base64.getDecoder.decode(fields(2)),
        Base64.getDecoder.decode(fields(3))
      )
    catch
      case error: IllegalArgumentException =>
        throw IllegalArgumentException(s"invalid credential hash at ${path.getFileName}:$line", error)

package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Base64
import scala.jdk.CollectionConverters.*

final class ScramCredentialStore private (
    private val values: Map[(SaslMechanism, String), ScramCredential]
):
  def credential(mechanism: SaslMechanism, user: String): Option[ScramCredential] = values.get(mechanism -> user)

  def principals: Set[String] = values.keysIterator.map(_._2).toSet

  def mechanisms: Set[SaslMechanism] = values.keysIterator.map(_._1).toSet

object ScramCredentialStore:
  def apply(values: Map[(SaslMechanism, String), ScramCredential]): ScramCredentialStore =
    new ScramCredentialStore(values)

object ScramCredentialFile:
  def load(path: Path): ScramCredentialStore =
    val entries = Files.readAllLines(path, StandardCharsets.UTF_8)
    val credentials = entries.asScala.iterator.zipWithIndex.foldLeft(Map.empty[(SaslMechanism, String), ScramCredential]) {
      case (current, (raw, index)) =>
        val line = raw.trim
        if line.isEmpty || line.startsWith("#") then current
        else
          val mechanismSeparator = line.indexOf(' ')
          val valueSeparator = line.indexOf('=', mechanismSeparator + 1)
          if mechanismSeparator <= 0 || valueSeparator <= mechanismSeparator + 1 then throw invalid(path, index + 1)
          val mechanism =
            try SaslMechanism.parse(line.substring(0, mechanismSeparator))
            catch case error: IllegalArgumentException => throw invalid(path, index + 1, error)
          if !mechanism.scram then throw invalid(path, index + 1)
          val user = line.substring(mechanismSeparator + 1, valueSeparator).trim
          try ScramIdentity.validate(user)
          catch case error: IllegalArgumentException => throw invalid(path, index + 1, error)
          if user.contains('=') then throw invalid(path, index + 1)
          val key = mechanism -> user
          if current.contains(key) then
            throw IllegalArgumentException(s"duplicate ${mechanism.wireName} credential for '$user'")
          current.updated(key, parseCredential(mechanism, line.substring(valueSeparator + 1).trim, path, index + 1))
    }
    ScramCredentialStore(credentials)

  def encode(mechanism: SaslMechanism, user: String, credential: ScramCredential): String =
    require(mechanism.scram && credential.mechanism == mechanism, "SCRAM mechanism does not match credential")
    ScramIdentity.validate(user)
    require(!user.contains('='), "invalid SCRAM user name")
    val encoder = Base64.getEncoder
    Vector(
      s"${mechanism.wireName} $user=${algorithm(mechanism)}",
      credential.iterations.toString,
      encoder.encodeToString(credential.salt),
      encoder.encodeToString(credential.storedKey),
      encoder.encodeToString(credential.serverKey)
    ).mkString("$")

  private def parseCredential(
      mechanism: SaslMechanism,
      value: String,
      path: Path,
      line: Int
  ): ScramCredential =
    val fields = value.split("\\$", -1)
    if fields.length != 5 || fields(0) != algorithm(mechanism) then throw invalid(path, line)
    try
      ScramCredential.fromKeys(
        mechanism,
        fields(1).toInt,
        Base64.getDecoder.decode(fields(2)),
        Base64.getDecoder.decode(fields(3)),
        Base64.getDecoder.decode(fields(4))
      )
    catch case error: IllegalArgumentException => throw invalid(path, line, error)

  private def algorithm(mechanism: SaslMechanism): String = mechanism match
    case SaslMechanism.ScramSha256 => "scram-sha-256"
    case SaslMechanism.ScramSha512 => "scram-sha-512"
    case mechanism                 => throw IllegalArgumentException(s"${mechanism.wireName} is not a SCRAM credential")

  private def invalid(path: Path, line: Int, cause: Throwable | Null = null): IllegalArgumentException =
    IllegalArgumentException(s"invalid SCRAM credential at ${path.getFileName}:$line", cause)

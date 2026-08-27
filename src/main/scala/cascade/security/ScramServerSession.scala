package cascade.security

import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.util.{Arrays, Base64}

sealed trait ScramStep
final case class ScramChallenge(bytes: Array[Byte]) extends ScramStep
final case class ScramSuccess(principal: String, bytes: Array[Byte]) extends ScramStep
final case class ScramFailure(message: String, bytes: Array[Byte]) extends ScramStep

final class ScramServerSession private[security] (
    mechanism: SaslMechanism,
    lookup: String => Option[ScramCredential],
    newServerNonce: () => String = ScramServerSession.secureNonce
):
  require(mechanism.scram, "SCRAM server session requires a SCRAM mechanism")

  private enum Phase:
    case Initial, Challenged, Complete

  private var phase = Phase.Initial
  private var principal = ""
  private var clientFirstBare = ""
  private var serverFirst = ""
  private var combinedNonce = ""
  private var expectedBinding = ""
  private var credential: ScramCredential | Null = null
  private var knownPrincipal = false

  def evaluate(token: Array[Byte]): ScramStep = synchronized {
    try
      phase match
        case Phase.Initial    => begin(token)
        case Phase.Challenged => finish(token)
        case Phase.Complete   => failure("SCRAM exchange is already complete", "e=other-error")
    catch
      case _: IllegalArgumentException => failure("invalid SCRAM message", "e=invalid-encoding")
  }

  private def begin(token: Array[Byte]): ScramStep =
    val first = ScramMessage.parseClientFirst(token)
    principal = first.user
    clientFirstBare = first.bare
    combinedNonce = first.nonce + newServerNonce()
    expectedBinding = Base64.getEncoder.encodeToString(first.gs2Header.getBytes(StandardCharsets.UTF_8))
    val selected = lookup(principal).filter(_.mechanism == mechanism)
    knownPrincipal = selected.nonEmpty
    credential = selected.getOrElse(ScramServerSession.syntheticCredential(mechanism))
    val active = credential.nn
    serverFirst = s"r=$combinedNonce,s=${Base64.getEncoder.encodeToString(active.salt)},i=${active.iterations}"
    phase = Phase.Challenged
    ScramChallenge(serverFirst.getBytes(StandardCharsets.UTF_8))

  private def finish(token: Array[Byte]): ScramStep =
    val last = ScramMessage.parseClientFinal(token)
    if last.channelBinding != expectedBinding || last.nonce != combinedNonce then
      failure("SCRAM channel binding or nonce does not match", "e=invalid-proof")
    else
      val authMessage = s"$clientFirstBare,$serverFirst,${last.withoutProof}"
      val signature = credential.nn.authenticate(authMessage, last.proof)
      Arrays.fill(last.proof, 0.toByte)
      if knownPrincipal && signature.nonEmpty then
        phase = Phase.Complete
        val encoded = Base64.getEncoder.encodeToString(signature.get)
        Arrays.fill(signature.get, 0.toByte)
        ScramSuccess(principal, s"v=$encoded".getBytes(StandardCharsets.UTF_8))
      else failure("SCRAM proof is invalid", "e=invalid-proof")

  private def failure(message: String, response: String): ScramFailure =
    phase = Phase.Complete
    credential = null
    ScramFailure(message, response.getBytes(StandardCharsets.UTF_8))

object ScramServerSession:
  private val random = SecureRandom()
  private val synthetic = SaslMechanism.Supported.filter(_.scram).map { mechanism =>
    val password = new Array[Char](32)
    val randomBytes = new Array[Byte](32)
    random.nextBytes(randomBytes)
    var index = 0
    while index < password.length do
      password(index) = ((randomBytes(index) & 0xff) % 94 + 33).toChar
      index += 1
    try mechanism -> ScramCredential.create(mechanism, password)
    finally
      Arrays.fill(password, '\u0000')
      Arrays.fill(randomBytes, 0.toByte)
  }.toMap

  private[security] def syntheticCredential(mechanism: SaslMechanism): ScramCredential = synthetic(mechanism)

  def apply(
      mechanism: SaslMechanism,
      lookup: String => Option[ScramCredential]
  ): ScramServerSession = new ScramServerSession(mechanism, lookup)

  private[security] def apply(
      mechanism: SaslMechanism,
      lookup: String => Option[ScramCredential],
      newServerNonce: () => String
  ): ScramServerSession = new ScramServerSession(mechanism, lookup, newServerNonce)

  private[security] def secureNonce(): String =
    val bytes = new Array[Byte](24)
    random.nextBytes(bytes)
    try Base64.getEncoder.withoutPadding().encodeToString(bytes)
    finally Arrays.fill(bytes, 0.toByte)

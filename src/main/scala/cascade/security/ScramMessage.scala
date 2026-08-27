package cascade.security

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.util.Base64

final case class ScramClientFirst(user: String, nonce: String, bare: String, gs2Header: String)

final case class ScramClientFinal(
    channelBinding: String,
    nonce: String,
    proof: Array[Byte],
    withoutProof: String
)

object ScramMessage:
  val MaximumMessageBytes = 16 * 1024
  val MaximumNonceChars = 512
  val MaximumAttributes = 16

  def parseClientFirst(bytes: Array[Byte]): ScramClientFirst =
    validateSize(bytes)
    val message = decodeUtf8(bytes)
    val firstComma = message.indexOf(',')
    val secondComma = if firstComma < 0 then -1 else message.indexOf(',', firstComma + 1)
    if firstComma < 0 || secondComma < 0 then invalid("SCRAM client-first message has no GS2 header")
    val flag = message.substring(0, firstComma)
    val authorizationId = message.substring(firstComma + 1, secondComma)
    if flag != "n" || authorizationId.nonEmpty then
      invalid("SCRAM channel binding and authorization identities are not supported")
    val gs2Header = message.substring(0, secondComma + 1)
    val bare = message.substring(secondComma + 1)
    val attributes = parseAttributes(bare)
    if attributes.contains('m') then invalid("SCRAM mandatory extensions are not supported")
    val user = attributes.get('n').map(decodeName).getOrElse(invalid("SCRAM user is missing"))
    val nonce = attributes.getOrElse('r', invalid("SCRAM nonce is missing"))
    ScramIdentity.validate(user)
    validateNonce(nonce)
    ScramClientFirst(user, nonce, bare, gs2Header)

  def parseClientFinal(bytes: Array[Byte]): ScramClientFinal =
    validateSize(bytes)
    val message = decodeUtf8(bytes)
    val proofMarker = message.lastIndexOf(",p=")
    if proofMarker <= 0 || message.indexOf(",p=") != proofMarker || message.substring(proofMarker + 3).contains(',') then
      invalid("SCRAM proof must be the final attribute")
    val withoutProof = message.substring(0, proofMarker)
    val attributes = parseAttributes(message)
    if attributes.contains('m') then invalid("SCRAM mandatory extensions are not supported")
    val binding = attributes.getOrElse('c', invalid("SCRAM channel binding is missing"))
    val nonce = attributes.getOrElse('r', invalid("SCRAM nonce is missing"))
    validateNonce(nonce)
    val proof =
      try Base64.getDecoder.decode(attributes.getOrElse('p', invalid("SCRAM proof is missing")))
      catch case _: IllegalArgumentException => invalid("SCRAM proof is not valid Base64")
    ScramClientFinal(binding, nonce, proof, withoutProof)

  def encodeName(value: String): String = value.replace("=", "=3D").replace(",", "=2C")

  private def decodeName(value: String): String =
    val result = StringBuilder(value.length)
    var index = 0
    while index < value.length do
      if value.charAt(index) == '=' then
        if index + 2 >= value.length then invalid("SCRAM user has an incomplete escape")
        value.substring(index, index + 3) match
          case "=2C" => result.append(',')
          case "=3D" => result.append('=')
          case _      => invalid("SCRAM user has an invalid escape")
        index += 3
      else
        result.append(value.charAt(index))
        index += 1
    result.result()

  private def parseAttributes(value: String): Map[Char, String] =
    if value.isEmpty then invalid("SCRAM attributes are empty")
    val parts = value.split(",", -1)
    if parts.length > MaximumAttributes then invalid("SCRAM message has too many attributes")
    parts.foldLeft(Map.empty[Char, String]) { (attributes, part) =>
      if part.length < 3 || part.charAt(1) != '=' then invalid("SCRAM attribute is malformed")
      val key = part.charAt(0)
      if !key.isLetter || attributes.contains(key) then invalid("SCRAM attribute is invalid or duplicated")
      attributes.updated(key, part.substring(2))
    }

  private def validateNonce(value: String): Unit =
    if value.isEmpty || value.length > MaximumNonceChars ||
        value.exists(character => character == ',' || character < 0x21 || character > 0x7e)
    then
      invalid("SCRAM nonce contains invalid characters")

  private def validateSize(bytes: Array[Byte]): Unit =
    if bytes.isEmpty || bytes.length > MaximumMessageBytes then invalid("SCRAM message size is outside policy")

  private def decodeUtf8(bytes: Array[Byte]): String =
    try
      StandardCharsets.UTF_8.newDecoder()
        .onMalformedInput(CodingErrorAction.REPORT)
        .onUnmappableCharacter(CodingErrorAction.REPORT)
        .decode(ByteBuffer.wrap(bytes))
        .toString
    catch case _: java.nio.charset.CharacterCodingException => invalid("SCRAM message is not valid UTF-8")

  private def invalid(message: String): Nothing = throw IllegalArgumentException(message)

object ScramIdentity:
  val MaximumChars = 255

  def validate(value: String): Unit =
    require(
      value.nonEmpty && value.length <= MaximumChars && value.forall(character => character >= 0x21 && character <= 0x7e),
      "SCRAM user must contain 1-255 visible ASCII characters"
    )

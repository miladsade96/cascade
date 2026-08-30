package cascade.security

import java.nio.ByteBuffer
import java.nio.charset.{CodingErrorAction, StandardCharsets}

private[security] sealed trait JsonValue

private[security] object JsonValue:
  final case class ObjectValue(fields: Map[String, JsonValue]) extends JsonValue
  final case class ArrayValue(values: Vector[JsonValue]) extends JsonValue
  final case class StringValue(value: String) extends JsonValue
  final case class NumberValue(value: BigDecimal) extends JsonValue
  final case class BooleanValue(value: Boolean) extends JsonValue
  case object NullValue extends JsonValue

private[security] object StrictJson:
  val MaximumDepth = 16
  val MaximumMembers = 1024
  val MaximumStringChars = 16 * 1024

  def parse(bytes: Array[Byte], maximumBytes: Int): JsonValue =
    require(maximumBytes > 0, "JSON byte limit must be positive")
    if bytes.isEmpty || bytes.length > maximumBytes then invalid("JSON size is outside policy")
    val source =
      try
        StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes))
          .toString
      catch case _: java.nio.charset.CharacterCodingException => invalid("JSON is not valid UTF-8")
    Parser(source).parseDocument()

  private final class Parser(source: String):
    private var index = 0
    private var members = 0

    def parseDocument(): JsonValue =
      skipWhitespace()
      val value = parseValue(0)
      skipWhitespace()
      if index != source.length then invalid("JSON has trailing content")
      value

    private def parseValue(depth: Int): JsonValue =
      if depth > MaximumDepth then invalid("JSON nesting is too deep")
      if index >= source.length then invalid("JSON value is missing")
      source.charAt(index) match
        case '{' => parseObject(depth + 1)
        case '[' => parseArray(depth + 1)
        case '"' => JsonValue.StringValue(parseString())
        case 't' => keyword("true", JsonValue.BooleanValue(true))
        case 'f' => keyword("false", JsonValue.BooleanValue(false))
        case 'n' => keyword("null", JsonValue.NullValue)
        case character if character == '-' || isDigit(character) => parseNumber()
        case _ => invalid("JSON value is invalid")

    private def parseObject(depth: Int): JsonValue.ObjectValue =
      index += 1
      skipWhitespace()
      var fields = Map.empty[String, JsonValue]
      if take('}') then return JsonValue.ObjectValue(fields)
      var complete = false
      while !complete do
        if index >= source.length || source.charAt(index) != '"' then invalid("JSON object key is missing")
        val key = parseString()
        if fields.contains(key) then invalid("JSON object contains a duplicate key")
        skipWhitespace()
        expect(':')
        skipWhitespace()
        countMember()
        fields = fields.updated(key, parseValue(depth))
        skipWhitespace()
        if take('}') then complete = true
        else
          expect(',')
          skipWhitespace()
      JsonValue.ObjectValue(fields)

    private def parseArray(depth: Int): JsonValue.ArrayValue =
      index += 1
      skipWhitespace()
      val values = Vector.newBuilder[JsonValue]
      if take(']') then return JsonValue.ArrayValue(Vector.empty)
      var complete = false
      while !complete do
        countMember()
        values += parseValue(depth)
        skipWhitespace()
        if take(']') then complete = true
        else
          expect(',')
          skipWhitespace()
      JsonValue.ArrayValue(values.result())

    private def parseString(): String =
      expect('"')
      val result = StringBuilder()
      var complete = false
      while !complete do
        if index >= source.length then invalid("JSON string is incomplete")
        val character = source.charAt(index)
        index += 1
        character match
          case '"' => complete = true
          case '\\' => appendEscape(result)
          case value if value < 0x20 => invalid("JSON string contains a control character")
          case value if Character.isHighSurrogate(value) =>
            if index >= source.length || !Character.isLowSurrogate(source.charAt(index)) then
              invalid("JSON string contains an invalid surrogate")
            result.append(value).append(source.charAt(index))
            index += 1
          case value if Character.isLowSurrogate(value) => invalid("JSON string contains an invalid surrogate")
          case value => result.append(value)
        if result.length > MaximumStringChars then invalid("JSON string is too long")
      result.result()

    private def appendEscape(result: StringBuilder): Unit =
      if index >= source.length then invalid("JSON escape is incomplete")
      val escaped = source.charAt(index)
      index += 1
      escaped match
        case '"' => result.append('"')
        case '\\' => result.append('\\')
        case '/' => result.append('/')
        case 'b' => result.append('\b')
        case 'f' => result.append('\f')
        case 'n' => result.append('\n')
        case 'r' => result.append('\r')
        case 't' => result.append('\t')
        case 'u' =>
          val first = unicodeEscape()
          if Character.isHighSurrogate(first) then
            if index + 1 >= source.length || source.charAt(index) != '\\' || source.charAt(index + 1) != 'u' then
              invalid("JSON unicode escape has no low surrogate")
            index += 2
            val second = unicodeEscape()
            if !Character.isLowSurrogate(second) then invalid("JSON unicode escape has an invalid low surrogate")
            result.append(first).append(second): Unit
          else if Character.isLowSurrogate(first) then invalid("JSON unicode escape has an unexpected low surrogate")
          else result.append(first)
        case _ => invalid("JSON escape is invalid")

    private def unicodeEscape(): Char =
      if index + 4 > source.length then invalid("JSON unicode escape is incomplete")
      var value = 0
      var count = 0
      while count < 4 do
        val digit = Character.digit(source.charAt(index + count), 16)
        if digit < 0 then invalid("JSON unicode escape is invalid")
        value = value * 16 + digit
        count += 1
      index += 4
      value.toChar

    private def parseNumber(): JsonValue.NumberValue =
      val start = index
      if take('-') && index >= source.length then invalid("JSON number is incomplete")
      if take('0') then
        if index < source.length && isDigit(source.charAt(index)) then invalid("JSON number has a leading zero")
      else
        if index >= source.length || !isDigit(source.charAt(index)) then invalid("JSON number is invalid")
        while index < source.length && isDigit(source.charAt(index)) do index += 1
      if take('.') then
        if index >= source.length || !isDigit(source.charAt(index)) then invalid("JSON fraction is incomplete")
        while index < source.length && isDigit(source.charAt(index)) do index += 1
      if index < source.length && (source.charAt(index) == 'e' || source.charAt(index) == 'E') then
        index += 1
        if index < source.length && (source.charAt(index) == '+' || source.charAt(index) == '-') then index += 1
        if index >= source.length || !isDigit(source.charAt(index)) then invalid("JSON exponent is incomplete")
        while index < source.length && isDigit(source.charAt(index)) do index += 1
      if index - start > 128 then invalid("JSON number is too long")
      val text = source.substring(start, index)
      try JsonValue.NumberValue(BigDecimal(text))
      catch case _: NumberFormatException => invalid("JSON number is outside policy")

    private def keyword(text: String, value: JsonValue): JsonValue =
      if !source.startsWith(text, index) then invalid("JSON keyword is invalid")
      index += text.length
      value

    private def skipWhitespace(): Unit =
      while index < source.length && isWhitespace(source.charAt(index)) do index += 1

    private def expect(expected: Char): Unit =
      if !take(expected) then invalid(s"JSON expected '$expected'")

    private def take(expected: Char): Boolean =
      if index < source.length && source.charAt(index) == expected then
        index += 1
        true
      else false

    private def countMember(): Unit =
      members += 1
      if members > MaximumMembers then invalid("JSON contains too many members")

    private def isDigit(character: Char): Boolean = character >= '0' && character <= '9'

    private def isWhitespace(character: Char): Boolean =
      character == ' ' || character == '\t' || character == '\r' || character == '\n'

  private def invalid(message: String): Nothing = throw IllegalArgumentException(message)

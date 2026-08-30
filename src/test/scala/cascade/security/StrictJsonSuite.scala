package cascade.security

import java.nio.charset.StandardCharsets

final class StrictJsonSuite extends munit.FunSuite:
  test("strict JSON parses nested UTF-8 values") {
    val bytes = """{"name":"cascade-\u2603","values":[1,true,null,{"x":"y"}]}""".getBytes(StandardCharsets.UTF_8)
    val parsed = StrictJson.parse(bytes, 1024)
    assert(parsed.isInstanceOf[JsonValue.ObjectValue])
  }

  test("strict JSON rejects duplicate members and trailing content") {
    intercept[IllegalArgumentException](parse("""{"kid":"one","kid":"two"}"""))
    intercept[IllegalArgumentException](parse("""{"kid":"one"} false"""))
  }

  test("strict JSON rejects invalid numbers, escapes, and nesting") {
    intercept[IllegalArgumentException](parse("[01]"))
    intercept[IllegalArgumentException](parse("""{"x":"\uD800"}"""))
    val nested = "[" * (StrictJson.MaximumDepth + 2) + "0" + "]" * (StrictJson.MaximumDepth + 2)
    intercept[IllegalArgumentException](parse(nested))
  }

  test("strict JSON rejects invalid UTF-8 and oversized input") {
    intercept[IllegalArgumentException](StrictJson.parse(Array(0xc3.toByte, 0x28.toByte), 10))
    intercept[IllegalArgumentException](StrictJson.parse("{}".getBytes(StandardCharsets.UTF_8), 1))
  }

  private def parse(value: String): JsonValue = StrictJson.parse(value.getBytes(StandardCharsets.UTF_8), 1024 * 1024)

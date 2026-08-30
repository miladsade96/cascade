package cascade.security

import java.nio.charset.StandardCharsets

final class OAuthBearerMessageSuite extends munit.FunSuite:
  test("OAUTHBEARER parses the Kafka RFC 7628 initial response") {
    val token = "header.claims.signature"
    val bytes = s"n,,\u0001host=broker.example\u0001auth=Bearer $token\u0001\u0001".getBytes(StandardCharsets.UTF_8)
    assertEquals(OAuthBearerMessage.parse(bytes, 1024), OAuthBearerRequest(token, None))
  }

  test("OAUTHBEARER decodes and returns a GS2 authorization identity") {
    val bytes = "n,a=alice=2Cops,\u0001auth=bEaReR a.b.c\u0001\u0001".getBytes(StandardCharsets.UTF_8)
    assertEquals(OAuthBearerMessage.parse(bytes, 1024).authorizationId, Some("alice,ops"))
  }

  test("OAUTHBEARER rejects malformed, duplicated, unterminated, and oversized messages") {
    val values = Vector(
      "n,,\u0001auth=Bearer a.b.c\u0001",
      "n,,\u0001auth=Bearer a.b.c\u0001auth=Bearer d.e.f\u0001\u0001",
      "p=tls-exporter,,\u0001auth=Bearer a.b.c\u0001\u0001",
      "n,,\u0001auth=Basic a.b.c\u0001\u0001"
    )
    values.foreach(value => intercept[IllegalArgumentException](OAuthBearerMessage.parse(value.getBytes(StandardCharsets.UTF_8), 1024)))
    intercept[IllegalArgumentException](OAuthBearerMessage.parse(Array.fill[Byte](6000)('a'.toByte), 1024))
    intercept[IllegalArgumentException](OAuthBearerMessage.parse(Array(0xc3.toByte, 0x28.toByte), 1024))
  }

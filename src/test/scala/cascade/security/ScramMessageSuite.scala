package cascade.security

import java.nio.charset.StandardCharsets

final class ScramMessageSuite extends munit.FunSuite:
  test("parses client-first names, nonce, and GS2 header without changing the signed bytes") {
    val message = "n,,n=user=2Cname=3Dprod,r=fyko+d2lbbFgONRv9qkxdawL"
    val parsed = ScramMessage.parseClientFirst(message.getBytes(StandardCharsets.UTF_8))

    assertEquals(parsed.user, "user,name=prod")
    assertEquals(parsed.nonce, "fyko+d2lbbFgONRv9qkxdawL")
    assertEquals(parsed.gs2Header, "n,,")
    assertEquals(parsed.bare, "n=user=2Cname=3Dprod,r=fyko+d2lbbFgONRv9qkxdawL")
    assertEquals(ScramMessage.encodeName(parsed.user), "user=2Cname=3Dprod")
  }

  test("parses client-final proof while preserving the proof-free signed bytes") {
    val message = "c=biws,r=client-server,x=value,p=dGVzdC1wcm9vZg=="
    val parsed = ScramMessage.parseClientFinal(message.getBytes(StandardCharsets.UTF_8))

    assertEquals(parsed.channelBinding, "biws")
    assertEquals(parsed.nonce, "client-server")
    assertEquals(String(parsed.proof, StandardCharsets.UTF_8), "test-proof")
    assertEquals(parsed.withoutProof, "c=biws,r=client-server,x=value")
  }

  test("rejects channel binding, authorization IDs, malformed names, nonces, and proofs") {
    Vector(
      "p=tls-server-end-point,,n=alice,r=nonce",
      "n,a=admin,n=alice,r=nonce",
      "n,,n=alice=XX,r=nonce",
      "n,,n=alice,r=bad nonce",
      "n,,n=alice,n=bob,r=nonce",
      "n,,m=required,n=alice,r=nonce"
    ).foreach(value => intercept[IllegalArgumentException](ScramMessage.parseClientFirst(value.getBytes(StandardCharsets.UTF_8))))

    Vector(
      "c=biws,r=nonce",
      "c=biws,r=nonce,p=not-base64!",
      "c=biws,r=nonce,p=dGVzdA==,x=late",
      "c=biws,r=nonce,p=dGVzdA==,p=dGVzdA=="
    ).foreach(value => intercept[IllegalArgumentException](ScramMessage.parseClientFinal(value.getBytes(StandardCharsets.UTF_8))))
  }

  test("bounds remote message, identity, nonce, and attribute sizes") {
    intercept[IllegalArgumentException](ScramMessage.parseClientFirst(new Array[Byte](ScramMessage.MaximumMessageBytes + 1)))
    intercept[IllegalArgumentException](
      ScramMessage.parseClientFirst(s"n,,n=${"a" * (ScramIdentity.MaximumChars + 1)},r=nonce".getBytes(StandardCharsets.UTF_8))
    )
    intercept[IllegalArgumentException](
      ScramMessage.parseClientFirst(s"n,,n=alice,r=${"n" * (ScramMessage.MaximumNonceChars + 1)}".getBytes(StandardCharsets.UTF_8))
    )
    val attributes = (0 until ScramMessage.MaximumAttributes).map(index => s"x=value$index").mkString(",")
    intercept[IllegalArgumentException](
      ScramMessage.parseClientFirst(s"n,,n=alice,r=nonce,$attributes".getBytes(StandardCharsets.UTF_8))
    )
    intercept[IllegalArgumentException](ScramIdentity.validate("álîce"))
  }

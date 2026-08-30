package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.Files

final class JwksSuite extends munit.FunSuite:
  test("JWKS parses an allowed RSA signing key") {
    val pair = OAuthTestSupport.keyPair()
    val set = JwtKeySet.parse(OAuthTestSupport.jwks(Vector("key-1" -> pair)).getBytes(StandardCharsets.UTF_8))
    assertEquals(set.keyIds, Set("key-1"))
    assert(set.resolve("key-1", JwtAlgorithm.Rs256).nonEmpty)
    assert(set.resolve("key-1", JwtAlgorithm.Rs512).isEmpty)
  }

  test("JWKS rejects weak, duplicate, and unusable keys") {
    val strong = OAuthTestSupport.keyPair()
    val duplicate = OAuthTestSupport.jwks(Vector("same" -> strong, "same" -> OAuthTestSupport.keyPair()))
    intercept[IllegalArgumentException](JwtKeySet.parse(duplicate.getBytes(StandardCharsets.UTF_8)))
    val weak = OAuthTestSupport.jwks(Vector("weak" -> OAuthTestSupport.keyPair(1024)))
    intercept[IllegalArgumentException](JwtKeySet.parse(weak.getBytes(StandardCharsets.UTF_8)))
    val unusable = OAuthTestSupport.jwks(Vector("key" -> strong)).replace("\"use\":\"sig\"", "\"use\":\"enc\"")
    intercept[IllegalArgumentException](JwtKeySet.parse(unusable.getBytes(StandardCharsets.UTF_8)))
  }

  test("reload keeps the last valid JWKS and recovers after atomic repair") {
    val directory = Files.createTempDirectory("cascade-jwks-reload")
    val path = directory.resolve("jwks.json")
    val first = OAuthTestSupport.keyPair()
    val second = OAuthTestSupport.keyPair()
    OAuthTestSupport.writeJwks(path, Vector("first" -> first))
    val source = ReloadableJwks(OAuthConfig(jwksUri = Some(path.toUri), jwksRefreshMillis = 0L))
    try
      assert(source.resolve("first", JwtAlgorithm.Rs256).nonEmpty)
      Files.writeString(path, "malformed", StandardCharsets.UTF_8): Unit
      assert(source.resolve("first", JwtAlgorithm.Rs256).nonEmpty)
      assert(source.lastReloadError.nonEmpty)
      OAuthTestSupport.writeJwks(path, Vector("second" -> second))
      assert(source.resolve("second", JwtAlgorithm.Rs256).nonEmpty)
      assert(source.resolve("first", JwtAlgorithm.Rs256).isEmpty)
      assertEquals(source.lastReloadError, None)
    finally
      source.close()
      SecurityTestSupport.deleteTree(directory)
  }

  test("scheduled JWKS refresh publishes rotated keys and reports malformed replacements") {
    val directory = Files.createTempDirectory("cascade-jwks-scheduled")
    val path = directory.resolve("jwks.json")
    val first = OAuthTestSupport.keyPair()
    val second = OAuthTestSupport.keyPair()
    OAuthTestSupport.writeJwks(path, Vector("first" -> first))
    val source = ReloadableJwks(OAuthConfig(jwksUri = Some(path.toUri), jwksRefreshMillis = 20L))
    try
      OAuthTestSupport.writeJwks(path, Vector("second" -> second))
      await(source.resolve("second", JwtAlgorithm.Rs256).nonEmpty)
      assert(source.resolve("first", JwtAlgorithm.Rs256).isEmpty)
      Files.writeString(path, "malformed", StandardCharsets.UTF_8): Unit
      await(source.lastReloadError.nonEmpty)
      assert(source.resolve("second", JwtAlgorithm.Rs256).nonEmpty)
      OAuthTestSupport.writeJwks(path, Vector("second" -> second))
      await(source.lastReloadError.isEmpty)
    finally
      source.close()
      SecurityTestSupport.deleteTree(directory)
  }

  test("initial malformed JWKS fails closed") {
    val directory = Files.createTempDirectory("cascade-jwks-invalid")
    val path = directory.resolve("jwks.json")
    Files.writeString(path, "malformed", StandardCharsets.UTF_8): Unit
    try intercept[IllegalArgumentException](ReloadableJwks(OAuthConfig(jwksUri = Some(path.toUri)))): Unit
    finally SecurityTestSupport.deleteTree(directory)
  }

  private def await(condition: => Boolean): Unit =
    val deadline = System.nanoTime() + 2_000_000_000L
    while !condition && System.nanoTime() < deadline do Thread.sleep(5L)
    assert(condition)

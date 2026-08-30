package cascade.security

import java.nio.file.Files
import java.time.Instant
import java.util.concurrent.{Executors, TimeUnit}

final class OAuthConcurrencySuite extends munit.FunSuite:
  test("shared OAuth verifier validates concurrently without mutable session state") {
    val directory = Files.createTempDirectory("cascade-oauth-concurrency")
    val path = directory.resolve("jwks.json")
    val pair = OAuthTestSupport.keyPair()
    OAuthTestSupport.writeJwks(path, Vector("active" -> pair))
    val now = Instant.now().getEpochSecond
    val config = OAuthConfig(
      jwksUri = Some(path.toUri),
      issuer = Some("https://issuer.example"),
      audience = Some("cascade"),
      requiredScopes = Set("cascade.read"),
      jwksRefreshMillis = 60000L
    )
    val keys = ReloadableJwks(config)
    val validator = JwtValidator(config, keys)
    val token = OAuthTestSupport.token(
      pair.getPrivate,
      "active",
      OAuthTestSupport.claims("https://issuer.example", "\"cascade\"", "alice", now - 1, now + 600)
    )
    val executor = Executors.newVirtualThreadPerTaskExecutor()
    try
      val futures = (0 until 8).map { _ =>
        executor.submit(() => (0 until 250).count(_ => validator.validate(token).isRight))
      }
      assertEquals(futures.map(_.get(30, TimeUnit.SECONDS)).sum, 2000)
    finally
      executor.shutdownNow(): Unit
      keys.close()
      SecurityTestSupport.deleteTree(directory)
  }

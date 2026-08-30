package cascade.security

import java.nio.file.Files
import java.time.{Clock, Instant, ZoneOffset}

final class JwtValidatorSuite extends munit.FunSuite:
  private val now = 2_000_000_000L

  test("JWT validator accepts signed issuer, audience, principal, and scope claims") {
    withValidator() { (pair, validator) =>
      val claims = OAuthTestSupport.claims(
        "https://issuer.example",
        "[\"other\",\"cascade\"]",
        "alice",
        now - 10,
        now + 300,
        "[\"cascade.read\",\"cascade.write\"]"
      )
      val result = validator.validate(OAuthTestSupport.token(pair.getPrivate, "active", claims))
      assertEquals(result.map(_.principal), Right("alice"))
      assert(result.exists(_.scopes.contains("cascade.write")))
    }
  }

  test("JWT validator rejects signature, key, algorithm, issuer, audience, and time failures") {
    withValidator() { (pair, validator) =>
      val valid = OAuthTestSupport.claims("https://issuer.example", "\"cascade\"", "alice", now - 1, now + 60)
      val other = OAuthTestSupport.keyPair()
      assertEquals(validator.validate(OAuthTestSupport.token(other.getPrivate, "active", valid)).left.toOption, Some(JwtValidationError.InvalidSignature))
      assertEquals(validator.validate(OAuthTestSupport.token(pair.getPrivate, "missing", valid)).left.toOption, Some(JwtValidationError.UnknownKey))
      val none = OAuthTestSupport.token(pair.getPrivate, "active", valid, tokenAlgorithm = Some("none"))
      assertEquals(validator.validate(none).left.toOption, Some(JwtValidationError.UnsupportedAlgorithm))
      val issuer = OAuthTestSupport.claims("https://wrong.example", "\"cascade\"", "alice", now - 1, now + 60)
      assertEquals(validator.validate(OAuthTestSupport.token(pair.getPrivate, "active", issuer)).left.toOption, Some(JwtValidationError.InvalidIssuer))
      val audience = OAuthTestSupport.claims("https://issuer.example", "\"wrong\"", "alice", now - 1, now + 60)
      assertEquals(validator.validate(OAuthTestSupport.token(pair.getPrivate, "active", audience)).left.toOption, Some(JwtValidationError.InvalidAudience))
      val expired = OAuthTestSupport.claims("https://issuer.example", "\"cascade\"", "alice", now - 100, now - 31)
      assertEquals(validator.validate(OAuthTestSupport.token(pair.getPrivate, "active", expired)).left.toOption, Some(JwtValidationError.Expired))
      val future = OAuthTestSupport.claims("https://issuer.example", "\"cascade\"", "alice", now + 31, now + 300)
      assertEquals(validator.validate(OAuthTestSupport.token(pair.getPrivate, "active", future)).left.toOption, Some(JwtValidationError.NotYetValid))
    }
  }

  test("JWT validator enforces principal and required scopes") {
    withValidator(requiredScopes = Set("cascade.admin")) { (pair, validator) =>
      val missing = OAuthTestSupport.claims("https://issuer.example", "\"cascade\"", "alice", now, now + 60)
      assertEquals(validator.validate(OAuthTestSupport.token(pair.getPrivate, "active", missing)).left.toOption, Some(JwtValidationError.MissingScope))
      val invalidPrincipal = OAuthTestSupport.claims(
        "https://issuer.example",
        "\"cascade\"",
        "bad principal",
        now,
        now + 60,
        "\"cascade.admin\""
      )
      assertEquals(validator.validate(OAuthTestSupport.token(pair.getPrivate, "active", invalidPrincipal)).left.toOption, Some(JwtValidationError.InvalidPrincipal))
    }
  }

  test("OAUTHBEARER authenticator requires the GS2 authorization identity to match the token principal") {
    withValidator() { (pair, validator) =>
      val config = OAuthConfig(maximumTokenBytes = 16 * 1024)
      val token = OAuthTestSupport.token(
        pair.getPrivate,
        "active",
        OAuthTestSupport.claims("https://issuer.example", "\"cascade\"", "alice", now, now + 60)
      )
      val authenticator = OAuthBearerAuthenticator(config, validator)
      val accepted = s"n,a=alice,\u0001auth=Bearer $token\u0001\u0001".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      val denied = s"n,a=bob,\u0001auth=Bearer $token\u0001\u0001".getBytes(java.nio.charset.StandardCharsets.UTF_8)
      assertEquals(authenticator.authenticate(accepted).map(_.principal), Some("alice"))
      assertEquals(authenticator.authenticate(denied), None)
    }
  }

  JwtAlgorithm.Supported.foreach { algorithm =>
    test(s"JWT validator supports explicitly allowed ${algorithm.jwtName}") {
      val directory = Files.createTempDirectory("cascade-jwt-algorithm")
      val path = directory.resolve("jwks.json")
      val pair = OAuthTestSupport.keyPair()
      OAuthTestSupport.writeJwks(path, Vector("active" -> pair), Some(algorithm.jwtName))
      val config = OAuthConfig(
        jwksUri = Some(path.toUri),
        issuer = Some("https://issuer.example"),
        audience = Some("cascade"),
        allowedAlgorithms = Set(algorithm),
        jwksRefreshMillis = 60000L
      )
      val keys = ReloadableJwks(config)
      try
        val claims = OAuthTestSupport.claims("https://issuer.example", "\"cascade\"", "alice", now, now + 60)
        val token = OAuthTestSupport.token(pair.getPrivate, "active", claims, algorithm)
        val validator = JwtValidator(config, keys, Clock.fixed(Instant.ofEpochSecond(now), ZoneOffset.UTC))
        assertEquals(validator.validate(token).map(_.principal), Right("alice"))
      finally
        keys.close()
        SecurityTestSupport.deleteTree(directory)
    }
  }

  private def withValidator(requiredScopes: Set[String] = Set("cascade.read"))(test: (java.security.KeyPair, JwtValidator) => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-jwt-validator")
    val path = directory.resolve("jwks.json")
    val pair = OAuthTestSupport.keyPair()
    OAuthTestSupport.writeJwks(path, Vector("active" -> pair))
    val config = OAuthConfig(
      jwksUri = Some(path.toUri),
      issuer = Some("https://issuer.example"),
      audience = Some("cascade"),
      requiredScopes = requiredScopes,
      jwksRefreshMillis = 60000L
    )
    val keys = ReloadableJwks(config)
    try test(pair, JwtValidator(config, keys, Clock.fixed(Instant.ofEpochSecond(now), ZoneOffset.UTC)))
    finally
      keys.close()
      SecurityTestSupport.deleteTree(directory)

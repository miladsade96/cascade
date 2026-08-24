package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.Files

final class PeerAuthenticatorSuite extends munit.FunSuite:
  test("accepts only a TLS certificate assigned to the claimed node") {
    val file = Files.createTempFile("cascade-peer-authenticator", ".conf")
    try
      Files.writeString(file, "1 CN=broker-1,O=Cascade\n2 CN=broker-2,O=Cascade\n", StandardCharsets.UTF_8): Unit
      val authenticator = PeerAuthenticator(
        PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(file), identityReloadIntervalMillis = 60_000L)
      )

      assertEquals(
        authenticator.authenticate(Some("cascade-peer:1"), session(secure = true, Some("CN=broker-1,O=Cascade"))),
        Right(AuthenticatedPeer(Some(1), "CN=broker-1,O=Cascade", encrypted = true))
      )
      assertEquals(
        authenticator.authenticate(Some("cascade-peer:2"), session(secure = true, Some("CN=broker-1,O=Cascade"))),
        Left("peer_identity_denied")
      )
      assertEquals(
        authenticator.authenticate(Some("cascade-peer:1"), session(secure = false, Some("CN=broker-1,O=Cascade"))),
        Left("peer_tls_required")
      )
      assertEquals(
        authenticator.authenticate(Some("cascade-peer:1"), session(secure = true, None)),
        Left("peer_certificate_required")
      )
      assertEquals(authenticator.authenticate(Some("cascade-peer:not-a-number"), session(true, None)), Left("invalid_peer_client_id"))
    finally Files.deleteIfExists(file): Unit
  }

  test("keeps legacy and node-aware client IDs compatible in plaintext mode") {
    val authenticator = PeerAuthenticator(PeerSecurityConfig())

    assert(authenticator.authenticate(Some("cascade-peer"), session(false, None)).isRight)
    assertEquals(
      authenticator.authenticate(Some("cascade-peer:3"), session(false, None)).map(_.nodeId),
      Right(Some(3))
    )
    assertEquals(authenticator.authenticate(Some("ordinary-client"), session(false, None)), Left("invalid_peer_client_id"))
  }

  private def session(secure: Boolean, principal: Option[String]): ConnectionSession =
    ConnectionSession("127.0.0.1", secure, authenticationRequired = false, principal)

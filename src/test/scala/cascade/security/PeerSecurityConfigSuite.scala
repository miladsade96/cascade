package cascade.security

import java.nio.file.Path

final class PeerSecurityConfigSuite extends munit.FunSuite:
  private val store = Path.of("broker.p12")
  private val identities = Path.of("peers.conf")

  test("accepts hostname-verified mutual TLS peer security") {
    val tls = TlsConfig(
      keyStore = Some(store),
      keyStorePassword = Some("store-password"),
      trustStore = Some(store),
      trustStorePassword = Some("store-password"),
      clientAuth = TlsClientAuth.Requested
    )
    val peer = PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(identities))

    assertEquals(peer.validate(SecurityProtocol.Ssl, tls), peer)
  }

  test("rejects peer SSL without listener TLS, client certificates, trust, or identities") {
    val peer = PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(identities))
    val base = TlsConfig(keyStore = Some(store), keyStorePassword = Some("store-password"))

    intercept[IllegalArgumentException](peer.validate(SecurityProtocol.Plaintext, base))
    intercept[IllegalArgumentException](peer.validate(SecurityProtocol.Ssl, base))
    intercept[IllegalArgumentException](
      peer.validate(SecurityProtocol.Ssl, base.copy(clientAuth = TlsClientAuth.Requested))
    )
    intercept[IllegalArgumentException](
      peer.copy(identityFile = None).validate(
        SecurityProtocol.Ssl,
        base.copy(
          clientAuth = TlsClientAuth.Requested,
          trustStore = Some(store),
          trustStorePassword = Some("store-password")
        )
      )
    )
  }

  test("rejects identity policy on plaintext peers and disabled hostname verification") {
    intercept[IllegalArgumentException](
      PeerSecurityConfig(identityFile = Some(identities)).validate(SecurityProtocol.Plaintext, TlsConfig())
    )
    intercept[IllegalArgumentException](PeerSecurityConfig(endpointIdentificationAlgorithm = ""))
  }

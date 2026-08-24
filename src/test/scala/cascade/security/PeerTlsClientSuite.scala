package cascade.security

import java.net.InetAddress
import java.nio.file.Files
import java.util.concurrent.{CompletableFuture, TimeUnit}
import javax.net.ssl.{SSLHandshakeException, SSLServerSocket, SSLSocket}

final class PeerTlsClientSuite extends munit.FunSuite:
  test("completes a hostname-verified mutually authenticated TLS handshake") {
    val directory = Files.createTempDirectory("cascade-peer-tls-client")
    try
      val keyStore = SecurityTestSupport.createKeyStore(directory)
      val tls = TlsConfig(
        keyStore = Some(keyStore),
        keyStorePassword = Some(SecurityTestSupport.StorePassword),
        trustStore = Some(keyStore),
        trustStorePassword = Some(SecurityTestSupport.StorePassword),
        clientAuth = TlsClientAuth.Required
      )
      val server = TlsContextFactory.create(tls).getServerSocketFactory
        .createServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
        .asInstanceOf[SSLServerSocket]
      server.setNeedClientAuth(true)
      val receivedPrincipal = CompletableFuture[String]()
      val acceptor = Thread.ofVirtual().start(() =>
        val accepted = server.accept().asInstanceOf[SSLSocket]
        try
          accepted.startHandshake()
          receivedPrincipal.complete(accepted.getSession.getPeerPrincipal.getName): Unit
        catch case error: Throwable => receivedPrincipal.completeExceptionally(error): Unit
        finally accepted.close()
      )
      try
        val client = PeerTlsClient(tls, PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(directory.resolve("peers.conf"))))
        val socket = client.connect("localhost", server.getLocalPort, 5000)
        try
          assert(socket.getSession.getProtocol.startsWith("TLSv1."))
          assert(socket.getSession.getPeerPrincipal.getName.contains("CN=localhost"))
          assert(receivedPrincipal.get(5, TimeUnit.SECONDS).contains("CN=localhost"))
        finally socket.close()
      finally
        server.close()
        acceptor.join(5000L)
    finally SecurityTestSupport.deleteTree(directory)
  }

  test("rejects a trusted certificate whose hostname does not match the target") {
    val directory = Files.createTempDirectory("cascade-peer-tls-hostname")
    try
      val keyStore = SecurityTestSupport.createKeyStore(
        directory,
        subjectAlternativeNames = "dns:localhost"
      )
      val tls = mutualTls(keyStore, keyStore)
      val (server, acceptor) = rejectingServer(tls)
      try
        val client = PeerTlsClient(tls, peerSecurity(directory))
        intercept[SSLHandshakeException](client.connect("127.0.0.1", server.getLocalPort, 5000))
      finally
        server.close()
        acceptor.join(5000L)
    finally SecurityTestSupport.deleteTree(directory)
  }

  test("rejects a certificate signed outside the configured trust store") {
    val directory = Files.createTempDirectory("cascade-peer-tls-untrusted")
    try
      val serverStore = SecurityTestSupport.createKeyStore(directory, "server.p12")
      val clientStore = SecurityTestSupport.createKeyStore(directory, "client.p12")
      val serverTls = mutualTls(serverStore, serverStore)
      val clientTls = mutualTls(clientStore, clientStore)
      val (server, acceptor) = rejectingServer(serverTls)
      try
        val client = PeerTlsClient(clientTls, peerSecurity(directory))
        intercept[SSLHandshakeException](client.connect("localhost", server.getLocalPort, 5000))
      finally
        server.close()
        acceptor.join(5000L)
    finally SecurityTestSupport.deleteTree(directory)
  }

  private def mutualTls(keyStore: java.nio.file.Path, trustStore: java.nio.file.Path): TlsConfig =
    TlsConfig(
      keyStore = Some(keyStore),
      keyStorePassword = Some(SecurityTestSupport.StorePassword),
      trustStore = Some(trustStore),
      trustStorePassword = Some(SecurityTestSupport.StorePassword),
      clientAuth = TlsClientAuth.Required
    )

  private def peerSecurity(directory: java.nio.file.Path): PeerSecurityConfig =
    PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(directory.resolve("peers.conf")))

  private def rejectingServer(tls: TlsConfig): (SSLServerSocket, Thread) =
    val server = TlsContextFactory.create(tls).getServerSocketFactory
      .createServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
      .asInstanceOf[SSLServerSocket]
    server.setNeedClientAuth(true)
    val acceptor = Thread.ofVirtual().start(() =>
      val accepted = server.accept().asInstanceOf[SSLSocket]
      try accepted.startHandshake()
      catch case _: Throwable => ()
      finally accepted.close()
    )
    (server, acceptor)

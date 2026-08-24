package cascade.security

import java.net.InetAddress
import java.nio.file.Files
import java.util.concurrent.{CompletableFuture, TimeUnit}
import javax.net.ssl.{SSLServerSocket, SSLSocket}

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

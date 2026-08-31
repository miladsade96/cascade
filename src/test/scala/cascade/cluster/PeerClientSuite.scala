package cascade.cluster

import cascade.protocol.ByteCursor
import cascade.security.{PeerSecurityConfig, PeerSecurityProtocol, ReloadableTlsContext, SecurityTestSupport, TlsClientAuth, TlsConfig, TlsContextFactory}
import java.io.{DataInputStream, DataOutputStream}
import java.net.{InetAddress, ServerSocket}
import java.nio.file.{Files, StandardCopyOption}
import java.util.concurrent.{CompletableFuture, LinkedBlockingQueue, TimeUnit}
import javax.net.ssl.{SSLServerSocket, SSLSocket}

final class PeerClientSuite extends munit.FunSuite:
  test("sends the local node claim and preserves response correlation") {
    val server = ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
    val clientId = CompletableFuture[String]()
    val acceptor = Thread.ofVirtual().start(() =>
      val socket = server.accept()
      try
        val input = DataInputStream(socket.getInputStream)
        val output = DataOutputStream(socket.getOutputStream)
        val frame = new Array[Byte](input.readInt())
        input.readFully(frame)
        val cursor = ByteCursor(frame)
        assertEquals(cursor.readShort(), InternalApi.Ping)
        assertEquals(cursor.readShort(), 0.toShort)
        val correlationId = cursor.readInt()
        clientId.complete(cursor.readNullableString().getOrElse("")): Unit
        output.writeInt(4)
        output.writeInt(correlationId)
        output.flush()
      catch case error: Throwable => clientId.completeExceptionally(error): Unit
      finally socket.close()
    )
    val client = PeerClient(localNodeId = 7)
    try
      val response = client.call(ClusterNode(1, "127.0.0.1", server.getLocalPort), InternalApi.Ping, Array.emptyByteArray, 5000)
      response.ensureFullyRead()
      assertEquals(clientId.get(5, TimeUnit.SECONDS), "cascade-peer:7")
    finally
      client.close()
      server.close()
      acceptor.join(5000L)
  }

  test("requires TLS material for an SSL peer client") {
    intercept[IllegalArgumentException](
      PeerClient(1, PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(java.nio.file.Path.of("peers.conf"))))
    )
  }

  test("negotiates peer release, metadata formats, and feature levels") {
    val server = ServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
    val acceptor = Thread.ofVirtual().start(() =>
      val socket = server.accept()
      try
        val input = DataInputStream(socket.getInputStream)
        val output = DataOutputStream(socket.getOutputStream)
        val frame = new Array[Byte](input.readInt())
        input.readFully(frame)
        val cursor = ByteCursor(frame)
        assertEquals(cursor.readShort(), InternalApi.PeerFeatures)
        assertEquals(cursor.readShort(), 0.toShort)
        val correlationId = cursor.readInt()
        cursor.readNullableString()
        cursor.ensureFullyRead()
        val body = cascade.protocol.ByteWriter().writeInt(correlationId).writeShort(0)
          .writeString("1.1.0").writeShort(1).writeShort(7)
        body.writeArray(Vector("consumer-v2" -> 1.toShort)) { case (name, level) =>
          body.writeString(name).writeShort(level): Unit
        }
        val response = body.result()
        output.writeInt(response.length)
        output.write(response)
        output.flush()
      finally socket.close()
    )
    val client = PeerClient(localNodeId = 7)
    try
      assertEquals(
        client.capabilities(ClusterNode(1, "127.0.0.1", server.getLocalPort), 5000),
        PeerCapabilities("1.1.0", 1, 7, Map("consumer-v2" -> 1.toShort))
      )
    finally
      client.close()
      server.close()
      acceptor.join(5000L)
  }

  test("reconnects a persistent peer channel with the new TLS generation") {
    val directory = Files.createTempDirectory("cascade-peer-client-tls-rotation")
    try
      val material = SecurityTestSupport.createMutualTlsMaterial(directory, Vector(1, 2))
      val activeKeyStore = directory.resolve("active-client.p12")
      Files.copy(material.keyStores(1), activeKeyStore)
      val tls = TlsConfig(
        keyStore = Some(activeKeyStore),
        keyStorePassword = Some(SecurityTestSupport.StorePassword),
        trustStore = Some(material.trustStore),
        trustStorePassword = Some(SecurityTestSupport.StorePassword),
        clientAuth = TlsClientAuth.Required,
        reloadIntervalMillis = 0L
      )
      val serverTls = tls.copy(keyStore = Some(material.keyStores(1)))
      val server = TlsContextFactory.create(serverTls).getServerSocketFactory
        .createServerSocket(0, 16, InetAddress.getByName("127.0.0.1"))
        .asInstanceOf[SSLServerSocket]
      server.setNeedClientAuth(true)
      val principals = LinkedBlockingQueue[String]()
      val acceptor = Thread.ofVirtual().start(() =>
        try
          (0 until 2).foreach { _ =>
            val socket = server.accept().asInstanceOf[SSLSocket]
            try
              socket.startHandshake()
              principals.put(socket.getSession.getPeerPrincipal.getName)
              servePeerRequest(socket)
            finally socket.close()
          }
        catch case _: java.net.SocketException => ()
      )
      val reloader = ReloadableTlsContext(tls)
      val peerSecurity = PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(directory.resolve("peers.conf")))
      val client = PeerClient(1, peerSecurity, Some(tls), Some(reloader))
      val node = ClusterNode(9, "localhost", server.getLocalPort)
      try
        client.call(node, InternalApi.Ping, Array.emptyByteArray, 5000).ensureFullyRead()
        assert(Option(principals.poll(5, TimeUnit.SECONDS)).exists(_.contains("CN=broker-1")))

        Files.copy(material.keyStores(2), activeKeyStore, StandardCopyOption.REPLACE_EXISTING)
        assert(reloader.reloadNow())
        client.call(node, InternalApi.Ping, Array.emptyByteArray, 5000).ensureFullyRead()
        assert(Option(principals.poll(5, TimeUnit.SECONDS)).exists(_.contains("CN=broker-2")))
      finally
        client.close()
        reloader.close()
        server.close()
        acceptor.join(5000L)
    finally SecurityTestSupport.deleteTree(directory)
  }

  private def servePeerRequest(socket: java.net.Socket): Unit =
    val input = DataInputStream(socket.getInputStream)
    val output = DataOutputStream(socket.getOutputStream)
    val frame = new Array[Byte](input.readInt())
    input.readFully(frame)
    val cursor = ByteCursor(frame)
    assertEquals(cursor.readShort(), InternalApi.Ping)
    assertEquals(cursor.readShort(), 0.toShort)
    val correlationId = cursor.readInt()
    cursor.readNullableString()
    cursor.ensureFullyRead()
    output.writeInt(4)
    output.writeInt(correlationId)
    output.flush()

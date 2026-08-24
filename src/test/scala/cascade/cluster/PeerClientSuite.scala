package cascade.cluster

import cascade.protocol.ByteCursor
import cascade.security.{PeerSecurityConfig, PeerSecurityProtocol}
import java.io.{DataInputStream, DataOutputStream}
import java.net.{InetAddress, ServerSocket}
import java.util.concurrent.{CompletableFuture, TimeUnit}

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

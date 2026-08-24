package cascade.broker

import cascade.cluster.{ClusterNode, InternalApi, PeerClient}
import cascade.security.*
import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.nio.file.Files

final class PeerSecurityIntegrationSuite extends munit.FunSuite:
  test("accepts the assigned certificate and rejects a certificate claiming another node") {
    val root = Files.createTempDirectory("cascade-peer-security-integration")
    val port = freePort()
    val node = ClusterNode(1, "localhost", port)
    val material = SecurityTestSupport.createMutualTlsMaterial(root, Vector(1, 2))
    val identities = root.resolve("peer-identities.conf")
    Files.writeString(
      identities,
      material.principals.toVector.sortBy(_._1).map { case (id, principal) => s"$id $principal" }.mkString("", "\n", "\n"),
      StandardCharsets.UTF_8
    ): Unit
    val audit = root.resolve("peer-audit.jsonl")
    val serverTls = tls(material.keyStores(1), material.trustStore)
    val peerSecurity = PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(identities), 0L)
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = port,
        advertisedHost = "localhost",
        advertisedPort = Some(port),
        dataDirectory = root.resolve("data"),
        nodeId = 1,
        clusterNodes = Vector(node),
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.Ssl,
          tls = serverTls,
          audit = AuditConfig(Some(audit)),
          peer = peerSecurity
        )
      )
    )
    try
      broker.start()
      val valid = PeerClient(1, peerSecurity, Some(tls(material.keyStores(1), material.trustStore)))
      try
        val response = valid.call(node, InternalApi.Ping, Array.emptyByteArray, 5000)
        assertEquals(response.readShort(), cascade.protocol.Errors.None)
        response.ensureFullyRead()
      finally valid.close()

      val impersonator = PeerClient(1, peerSecurity, Some(tls(material.keyStores(2), material.trustStore)))
      try intercept[java.io.EOFException](impersonator.call(node, InternalApi.Ping, Array.emptyByteArray, 5000))
      finally impersonator.close()
    finally
      broker.close()
      val events = Files.readString(audit, StandardCharsets.UTF_8)
      assert(events.contains("\"event\":\"peer_authentication\""))
      assert(events.contains("\"decision\":\"allowed\""))
      assert(events.contains("\"decision\":\"denied\""))
      assert(events.contains("\"resource\":\"node-1\""))
      SecurityTestSupport.deleteTree(root)
  }

  test("rotates an assigned peer certificate without restarting the broker") {
    val root = Files.createTempDirectory("cascade-peer-identity-rotation")
    val port = freePort()
    val node = ClusterNode(1, "localhost", port)
    val material = SecurityTestSupport.createMutualTlsMaterial(root, Vector(1, 2))
    val identities = root.resolve("peer-identities.conf")
    Files.writeString(identities, s"1 ${material.principals(1)}\n", StandardCharsets.UTF_8): Unit
    val peerSecurity = PeerSecurityConfig(PeerSecurityProtocol.Ssl, Some(identities), 0L)
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = port,
        advertisedHost = "localhost",
        advertisedPort = Some(port),
        dataDirectory = root.resolve("data"),
        nodeId = 1,
        security = BrokerSecurityConfig(
          protocol = SecurityProtocol.Ssl,
          tls = tls(material.keyStores(1), material.trustStore),
          peer = peerSecurity
        )
      )
    )
    try
      broker.start()
      val original = PeerClient(1, peerSecurity, Some(tls(material.keyStores(1), material.trustStore)))
      try assertPing(original, node)
      finally original.close()

      val premature = PeerClient(1, peerSecurity, Some(tls(material.keyStores(2), material.trustStore)))
      try intercept[java.io.EOFException](premature.call(node, InternalApi.Ping, Array.emptyByteArray, 5000))
      finally premature.close()

      Files.writeString(identities, s"1 ${material.principals(2)}\n", StandardCharsets.UTF_8): Unit
      val rotated = PeerClient(1, peerSecurity, Some(tls(material.keyStores(2), material.trustStore)))
      try assertPing(rotated, node)
      finally rotated.close()

      assertEquals(broker.metricsSnapshot.peerSecurity.authenticated, 2L)
      assertEquals(broker.metricsSnapshot.peerSecurity.tlsAuthenticated, 2L)
      assertEquals(broker.metricsSnapshot.peerSecurity.rejected, 1L)
    finally
      broker.close()
      SecurityTestSupport.deleteTree(root)
  }

  private def assertPing(client: PeerClient, node: ClusterNode): Unit =
    val response = client.call(node, InternalApi.Ping, Array.emptyByteArray, 5000)
    assertEquals(response.readShort(), cascade.protocol.Errors.None)
    response.ensureFullyRead()

  private def tls(keyStore: java.nio.file.Path, trustStore: java.nio.file.Path): TlsConfig =
    TlsConfig(
      keyStore = Some(keyStore),
      keyStorePassword = Some(SecurityTestSupport.StorePassword),
      trustStore = Some(trustStore),
      trustStorePassword = Some(SecurityTestSupport.StorePassword),
      clientAuth = TlsClientAuth.Requested
    )

  private def freePort(): Int =
    val socket = ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()

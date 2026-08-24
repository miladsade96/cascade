package cascade.broker

import cascade.storage.{CleanupPolicy, FlushPolicy}
import cascade.security.{PeerSecurityProtocol, SecurityProtocol, TlsClientAuth}
import java.nio.file.Files
import munit.FunSuite

final class BrokerConfigSuite extends FunSuite:
  test("parses flush durability settings") {
    val config = BrokerConfig.parse(
      Array(
        "--flush-policy",
        "sync",
        "--flush-interval-ms",
        "250",
        "--flush-bytes",
        "1048576"
      )
    )

    assertEquals(config.flushPolicy, FlushPolicy.Sync)
    assertEquals(config.flushIntervalMillis, 250L)
    assertEquals(config.flushBytes, 1_048_576L)
  }

  test("rejects an unknown flush policy") {
    intercept[IllegalArgumentException] {
      BrokerConfig.parse(Array("--flush-policy", "eventually"))
    }
  }

  test("parses a static metadata and replication quorum") {
    val config = BrokerConfig.parse(
      Array(
        "--node-id",
        "2",
        "--cluster-nodes",
        "1@node-a:9092,2@node-b:9092,3@node-c:9092",
        "--controller-id",
        "1",
        "--default-replication-factor",
        "3",
        "--min-insync-replicas",
        "2",
        "--peer-timeout-ms",
        "1500",
        "--replica-recovery-timeout-ms",
        "120000",
        "--replica-recovery-chunk-bytes",
        "4194304",
        "--controller-heartbeat-ms",
        "200",
        "--controller-election-timeout-ms",
        "1000"
      )
    )

    assertEquals(config.nodeId, 2)
    assertEquals(config.clusterNodes.map(_.id), Vector(1, 2, 3))
    assertEquals(config.defaultReplicationFactor, 3)
    assertEquals(config.minInSyncReplicas, 2)
    assertEquals(config.peerTimeoutMillis, 1500)
    assertEquals(config.replicaRecoveryTimeoutMillis, 120000)
    assertEquals(config.replicaRecoveryChunkBytes, 4_194_304)
    assertEquals(config.controllerHeartbeatMillis, 200)
    assertEquals(config.controllerElectionTimeoutMillis, 1000)
  }

  test("allows a new observer to discover a quorum before it becomes a voter") {
    val config = BrokerConfig(
      nodeId = 4,
      advertisedHost = "node-d",
      clusterNodes = Vector(
        cascade.cluster.ClusterNode(1, "node-a", 9092),
        cascade.cluster.ClusterNode(2, "node-b", 9092),
        cascade.cluster.ClusterNode(3, "node-c", 9092)
      ),
      controllerId = 1,
      defaultReplicationFactor = 3
    )

    assertEquals(config.nodeId, 4)
    assert(!config.clusterNodes.exists(_.id == config.nodeId))
  }

  test("parses storage lifecycle and disk-pressure settings") {
    val config = BrokerConfig.parse(
      Array(
        "--cleanup-policy",
        "compact,delete",
        "--retention-ms",
        "3600000",
        "--retention-bytes",
        "1073741824",
        "--lifecycle-interval-ms",
        "30000",
        "--minimum-free-bytes",
        "536870912",
        "--offset-retention-ms",
        "86400000",
        "--journal-compaction-bytes",
        "1048576"
      )
    )

    assertEquals(config.storageLifecycle.cleanupPolicy, CleanupPolicy.CompactDelete)
    assertEquals(config.storageLifecycle.retentionMillis, 3_600_000L)
    assertEquals(config.storageLifecycle.retentionBytes, 1_073_741_824L)
    assertEquals(config.storageLifecycle.lifecycleIntervalMillis, 30_000L)
    assertEquals(config.storageLifecycle.minimumFreeBytes, 536_870_912L)
    assertEquals(config.storageLifecycle.offsetRetentionMillis, 86_400_000L)
    assertEquals(config.storageLifecycle.journalCompactionBytes, 1_048_576L)
  }

  test("rejects invalid lifecycle settings") {
    intercept[IllegalArgumentException](BrokerConfig.parse(Array("--cleanup-policy", "archive")))
    intercept[IllegalArgumentException](BrokerConfig.parse(Array("--retention-ms", "0")))
    intercept[IllegalArgumentException](BrokerConfig.parse(Array("--minimum-free-bytes", "-1")))
  }

  test("parses security and resource-isolation settings without exposing secrets on the command line") {
    val password = Files.createTempFile("cascade-keystore", ".password")
    val keyPassword = Files.createTempFile("cascade-key", ".password")
    val trustPassword = Files.createTempFile("cascade-truststore", ".password")
    Files.writeString(password, "store-secret\n")
    Files.writeString(keyPassword, "key-secret\r\n")
    Files.writeString(trustPassword, "trust-secret")
    try
      val config = BrokerConfig.parse(
        Array(
          "--security-protocol", "SASL_SSL",
          "--ssl-keystore", "broker.p12",
          "--ssl-keystore-password-file", password.toString,
          "--ssl-key-password-file", keyPassword.toString,
          "--ssl-truststore", "clients.p12",
          "--ssl-truststore-password-file", trustPassword.toString,
          "--ssl-client-auth", "requested",
          "--tls-protocols", "TLSv1.3,TLSv1.2",
          "--peer-security-protocol", "SSL",
          "--peer-identity-file", "peers.conf",
          "--peer-identity-reload-ms", "400",
          "--credentials-file", "users.conf",
          "--credential-reload-ms", "250",
          "--sasl-session-lifetime-ms", "3600000",
          "--acl-file", "acls.conf",
          "--acl-reload-ms", "300",
          "--super-users", "admin,operator",
          "--audit-log", "audit.jsonl",
          "--audit-buffered",
          "--max-connections", "500",
          "--max-connections-per-ip", "50",
          "--max-inflight-requests", "250",
          "--request-bytes-per-second", "1048576",
          "--request-burst-bytes", "2097152",
          "--max-throttle-ms", "750"
        )
      )

      assertEquals(config.security.protocol, SecurityProtocol.SaslSsl)
      assertEquals(config.security.tls.keyStorePassword, Some("store-secret"))
      assertEquals(config.security.tls.keyPassword, Some("key-secret"))
      assertEquals(config.security.tls.trustStorePassword, Some("trust-secret"))
      assertEquals(config.security.tls.clientAuth, TlsClientAuth.Requested)
      assertEquals(config.security.peer.protocol, PeerSecurityProtocol.Ssl)
      assertEquals(config.security.peer.identityFile.map(_.toString), Some("peers.conf"))
      assertEquals(config.security.peer.identityReloadIntervalMillis, 400L)
      assertEquals(config.security.authorization.superUsers, Set("admin", "operator"))
      assertEquals(config.security.resources.maxConnections, 500)
      assertEquals(config.security.resources.requestBytesPerSecond, 1_048_576L)
      assert(!config.security.audit.forceEachEvent)
    finally
      Files.deleteIfExists(password): Unit
      Files.deleteIfExists(keyPassword): Unit
      Files.deleteIfExists(trustPassword): Unit
  }

  test("validates TLS and SASL dependencies after all options are parsed") {
    intercept[IllegalArgumentException](BrokerConfig.parse(Array("--security-protocol", "SSL")))
    intercept[IllegalArgumentException](BrokerConfig.parse(Array("--security-protocol", "SASL_PLAINTEXT")))
    intercept[IllegalArgumentException](
      BrokerConfig.parse(Array("--peer-security-protocol", "SSL", "--peer-identity-file", "peers.conf"))
    )
  }

  test("parses operations, readiness, logging, and capacity settings") {
    val token = Files.createTempFile("cascade-operations", ".token")
    Files.writeString(token, "a-secure-operations-token-with-32-characters")
    try
      val config = BrokerConfig.parse(
        Array(
          "--operations-host", "0.0.0.0",
          "--operations-port", "9404",
          "--operations-token-file", token.toString,
          "--structured-log", "broker.jsonl",
          "--structured-log-max-bytes", "1048576",
          "--structured-log-retained-files", "3",
          "--no-stderr-log",
          "--readiness-max-pending-flush-bytes", "67108864",
          "--capacity-alert-interval-ms", "5000",
          "--capacity-connection-ratio", "0.75",
          "--capacity-inflight-ratio", "0.80",
          "--capacity-pending-flush-bytes", "33554432",
          "--capacity-minimum-free-bytes", "1073741824",
          "--capacity-alert-repeat-ms", "60000"
        )
      )
      assertEquals(config.operations.bindHost, "0.0.0.0")
      assertEquals(config.operations.port, Some(9404))
      assertEquals(config.operations.authenticationToken, Some("a-secure-operations-token-with-32-characters"))
      assertEquals(config.operations.structuredLogRetainedFiles, 3)
      assertEquals(config.operations.capacityAlerts.connectionUtilization, 0.75d)
      assertEquals(config.operations.capacityAlerts.minimumFreeBytes, 1_073_741_824L)
      assert(!config.operations.logToStderr)
    finally Files.deleteIfExists(token): Unit
  }

  test("requires authentication when operations bind beyond loopback") {
    intercept[IllegalArgumentException] {
      BrokerConfig.parse(Array("--operations-host", "0.0.0.0", "--operations-port", "9404"))
    }
  }

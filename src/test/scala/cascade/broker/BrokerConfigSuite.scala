package cascade.broker

import cascade.storage.FlushPolicy
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

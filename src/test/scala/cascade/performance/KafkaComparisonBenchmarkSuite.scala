package cascade.performance

import munit.FunSuite

final class KafkaComparisonBenchmarkSuite extends FunSuite:
  test("comparison configuration captures a fair workload") {
    val config = KafkaComparisonConfig.parse(Array(
      "--engine", "cascade",
      "--bootstrap", "127.0.0.1:19092",
      "--records", "10000",
      "--warmup-records", "1000",
      "--payload-bytes", "512",
      "--partitions", "4",
      "--replication-factor", "1",
      "--producers", "2",
      "--compression", "none",
      "--acks", "all"
    ))
    assertEquals(config.records, 10000)
    assertEquals(config.warmupRecords, 1000)
    assertEquals(config.payloadBytes, 512)
    assertEquals(config.partitions, 4)
    assertEquals(config.replicationFactor, 1.toShort)
    assertEquals(config.producers, 2)
    assertEquals(config.compression, "none")
    assertEquals(config.acks, "all")
  }

  test("comparison result records exactness and environment") {
    val result = KafkaComparisonResult(
      "cascade", 1000, 100, 1024, 8, 1, 4, "lz4", "all", 100, 200, 300,
      5000.0, 3333.333, 100, 200, 300, 1000, 0, 0, 64.5, "21", "3.3.8", "test-os", 8
    )
    assert(result.json.contains("\"lost\":0"))
    assert(result.json.contains("\"unexpected_duplicates\":0"))
    assert(result.json.contains("\"replication_factor\":1"))
  }

  test("comparison requires engine and bootstrap first") {
    intercept[IllegalArgumentException](KafkaComparisonConfig.parse(Array("--records", "10")))
  }


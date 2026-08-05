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

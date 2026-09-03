package cascade.group

import cascade.broker.BrokerConfig
import munit.FunSuite

final class OffsetBatchConfigSuite extends FunSuite:
  test("batch defaults are bounded and CLI supports a single-request control run") {
    val defaults = OffsetBatchConfig()
    assertEquals(defaults.maxRequests, 64)
    assertEquals(defaults.lingerMillis, 2L)
    val config = BrokerConfig.parse(Array("--offset-batch-max-requests", "1", "--offset-batch-linger-ms", "0",
      "--offset-batch-pending-requests", "32", "--offset-batch-max-bytes", "4096",
      "--offset-batch-pending-bytes", "8192", "--offset-batch-queue-timeout-ms", "1000"))
    assertEquals(config.offsetBatch, OffsetBatchConfig(1, 4096L, 32, 8192L, 0L, 1000L))
  }

  test("unsafe bounds fail before creating threads or accepting traffic") {
    val invalid = Vector[() => OffsetBatchConfig](
      () => OffsetBatchConfig(maxRequests = 0), () => OffsetBatchConfig(maxRequests = 1025),
      () => OffsetBatchConfig(maxBytes = 1023L), () => OffsetBatchConfig(maxBytes = 16777217L),
      () => OffsetBatchConfig(maxPendingRequests = 63), () => OffsetBatchConfig(maxPendingRequests = 65537),
      () => OffsetBatchConfig(maxPendingBytes = 1024L), () => OffsetBatchConfig(maxPendingBytes = Long.MaxValue),
      () => OffsetBatchConfig(lingerMillis = -1L), () => OffsetBatchConfig(lingerMillis = 101L),
      () => OffsetBatchConfig(queueTimeoutMillis = 2L), () => OffsetBatchConfig(queueTimeoutMillis = 60001L)
    )
    invalid.foreach(value => intercept[IllegalArgumentException](value()))
  }

  test("retained-byte accounting includes metadata and uses wide arithmetic") {
    val small = OffsetCommitCommand("a", -1, "", None, Vector.empty)
    val large = small.copy(values = Vector(OffsetCommitValue(GroupOffsetKey("a", "topic", 0),
      CommittedOffset(0L, -1, Some("x" * 10000), 0L))))
    assert(large.retainedBytes >= small.retainedBytes + 20000L)
  }

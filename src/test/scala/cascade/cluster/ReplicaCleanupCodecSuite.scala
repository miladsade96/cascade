package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.storage.{PartitionCleanupDecision, PartitionCleanupPlan}
import munit.FunSuite

final class ReplicaCleanupCodecSuite extends FunSuite:
  test("replica cleanup requests round trip every fencing and plan field") {
    val request = ReplicaCleanupRequest(
      "events",
      3,
      7,
      PartitionCleanupDecision(11, 42L, PartitionCleanupPlan(100L, 150L, 9000L))
    )
    assertEquals(ReplicaCleanupCodec.decode(ByteCursor(ReplicaCleanupCodec.encode(request))), request)
  }

  test("replica cleanup decoding rejects trailing and truncated payloads") {
    val request = ReplicaCleanupRequest(
      "events",
      0,
      1,
      PartitionCleanupDecision(2, 1L, PartitionCleanupPlan(0L, 10L, 100L))
    )
    val encoded = ReplicaCleanupCodec.encode(request)
    intercept[ProtocolException](ReplicaCleanupCodec.decode(ByteCursor(encoded :+ 1.toByte)))
    intercept[ProtocolException](ReplicaCleanupCodec.decode(ByteCursor(encoded.dropRight(1))))
  }

  test("replica cleanup decoding validates offsets and generations") {
    val invalidGeneration = ByteWriter()
      .writeString("events")
      .writeInt(0)
      .writeInt(1)
      .writeInt(2)
      .writeLong(0L)
      .writeLong(0L)
      .writeLong(10L)
      .writeLong(100L)
      .result()
    intercept[IllegalArgumentException](ReplicaCleanupCodec.decode(ByteCursor(invalidGeneration)))
  }

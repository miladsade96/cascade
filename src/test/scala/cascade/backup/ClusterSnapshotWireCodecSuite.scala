package cascade.backup

import cascade.protocol.{ByteCursor, ProtocolException}
import java.util.UUID
import munit.FunSuite

final class ClusterSnapshotWireCodecSuite extends FunSuite:
  private val identity = ClusterSnapshotIdentity(
    UUID.fromString("12345678-1234-5678-9234-567812345678"), 1, 9L, 55L, "a" * 64
  )

  test("prepare, capture and release requests round trip their fencing fields") {
    val prepare = SnapshotPrepareRequest(identity, 30_000L)
    assertEquals(ClusterSnapshotWireCodec.decodePrepare(ByteCursor(ClusterSnapshotWireCodec.encodePrepare(prepare))), prepare)
    val capture = SnapshotCaptureRequest(identity, "/mnt/off-host/generation-1/node-1")
    assertEquals(ClusterSnapshotWireCodec.decodeCapture(ByteCursor(ClusterSnapshotWireCodec.encodeCapture(capture))), capture)
    assertEquals(
      ClusterSnapshotWireCodec.decodeRelease(ByteCursor(ClusterSnapshotWireCodec.encodeRelease(identity.snapshotId))),
      identity.snapshotId
    )
  }

  test("node artifacts round trip exact watermarks") {
    val artifact = NodeSnapshotArtifact(
      1,
      "11111111-1111-4111-8111-111111111111",
      "node-1",
      "b" * 64,
      7,
      4096L,
      Vector(SnapshotWatermark("orders", 0, 10L), SnapshotWatermark("orders", 1, 12L))
    )
    assertEquals(ClusterSnapshotWireCodec.decodeArtifact(ByteCursor(ClusterSnapshotWireCodec.encodeArtifact(artifact))), artifact)
  }

  test("wire decoders reject trailing, truncated and invalid payloads") {
    val encoded = ClusterSnapshotWireCodec.encodePrepare(SnapshotPrepareRequest(identity, 1000L))
    intercept[ProtocolException](ClusterSnapshotWireCodec.decodePrepare(ByteCursor(encoded :+ 1.toByte))): Unit
    intercept[ProtocolException](ClusterSnapshotWireCodec.decodePrepare(ByteCursor(encoded.dropRight(1)))): Unit
    val invalid = encoded.dropRight(java.lang.Long.BYTES) ++ Array.fill[Byte](java.lang.Long.BYTES)(0)
    intercept[IllegalArgumentException](ClusterSnapshotWireCodec.decodePrepare(ByteCursor(invalid))): Unit
  }

package cascade.backup

import java.nio.file.Files
import java.time.Instant
import java.util.UUID
import munit.FunSuite

final class ClusterSnapshotManifestSuite extends FunSuite:
  private val identity = ClusterSnapshotIdentity(
    UUID.fromString("12345678-1234-5678-9234-567812345678"),
    1,
    7L,
    42L,
    "a" * 64
  )

  test("round trips sorted node artifacts and watermarks") {
    val manifest = ClusterSnapshotManifest(
      identity,
      Instant.parse("2026-10-21T08:00:00Z"),
      Vector(
        artifact(2, "22222222-2222-4222-8222-222222222222", Vector(SnapshotWatermark("orders", 0, 10L))),
        artifact(1, "11111111-1111-4111-8111-111111111111", Vector(
          SnapshotWatermark("orders", 1, 11L),
          SnapshotWatermark("orders", 0, 10L)
        ))
      )
    )
    val encoded = ClusterSnapshotManifestCodec.encode(manifest)
    assertEquals(ClusterSnapshotManifestCodec.decode(encoded), manifest.copy(nodes = manifest.nodes.sortBy(_.nodeId).map(
      node => node.copy(watermarks = node.watermarks.sortBy(value => value.topic -> value.partition))
    )))
  }

  test("atomically publishes exactly one cluster manifest") {
    val root = Files.createTempDirectory("cascade-cluster-manifest")
    try
      val manifest = ClusterSnapshotManifest(
        identity,
        Instant.EPOCH,
        Vector(artifact(1, "11111111-1111-4111-8111-111111111111", Vector.empty))
      )
      val path = ClusterSnapshotManifestCodec.writeAtomic(root, manifest)
      assertEquals(ClusterSnapshotManifestCodec.read(path), manifest)
      intercept[IllegalArgumentException](ClusterSnapshotManifestCodec.writeAtomic(root, manifest)): Unit
      val stream = Files.list(root)
      try assertEquals(stream.toArray.map(_.asInstanceOf[java.nio.file.Path].getFileName.toString).toSet,
        Set(ClusterSnapshotManifestCodec.FileName))
      finally stream.close()
    finally deleteTree(root)
  }

  test("rejects duplicate nodes, unsafe bindings, malformed counts and trailing data") {
    val first = artifact(1, "11111111-1111-4111-8111-111111111111", Vector.empty)
    intercept[IllegalArgumentException](ClusterSnapshotManifest(identity, Instant.EPOCH, Vector(first, first))): Unit
    intercept[IllegalArgumentException](first.copy(artifactDirectory = "node-2")): Unit
    intercept[IllegalArgumentException](first.copy(manifestSha256 = "bad")): Unit
    val valid = ClusterSnapshotManifest(identity, Instant.EPOCH, Vector(first))
    val encoded = ClusterSnapshotManifestCodec.encode(valid)
    intercept[IllegalArgumentException](ClusterSnapshotManifestCodec.decode(encoded.replace("node_count\t1", "node_count\t2"))): Unit
    intercept[IllegalArgumentException](ClusterSnapshotManifestCodec.decode(encoded + "unexpected\n")): Unit
  }

  private def artifact(nodeId: Int, directoryId: String, watermarks: Vector[SnapshotWatermark]): NodeSnapshotArtifact =
    NodeSnapshotArtifact(nodeId, directoryId, s"node-$nodeId", "b" * 64, 3, 128L, watermarks)

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path): Unit)
      finally paths.close()

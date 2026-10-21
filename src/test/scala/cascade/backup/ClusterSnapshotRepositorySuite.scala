package cascade.backup

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import java.util.UUID
import munit.FunSuite

final class ClusterSnapshotRepositorySuite extends FunSuite:
  test("verifies the exact collected cluster artifact set") {
    val root = Files.createTempDirectory("cascade-cluster-snapshot-verify")
    try
      val generation = createGeneration(root, "generation-1", Instant.parse("2026-10-21T01:00:00Z"), 1)
      val verified = ClusterSnapshotRepository.verify(generation)
      assertEquals(verified.manifest.nodes.size, 1)
      assert(verified.totalBytes > 0L)
      Files.writeString(generation.resolve("unexpected"), "bad", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](ClusterSnapshotRepository.verify(generation)): Unit
    finally deleteTree(root)
  }

  test("retention deletes only old verified generations with enough verified replacements") {
    val root = Files.createTempDirectory("cascade-cluster-snapshot-retention")
    try
      val first = createGeneration(root, "generation-1", Instant.parse("2026-10-21T01:00:00Z"), 1)
      val second = createGeneration(root, "generation-2", Instant.parse("2026-10-21T02:00:00Z"), 2)
      val third = createGeneration(root, "generation-3", Instant.parse("2026-10-21T03:00:00Z"), 3)
      val incomplete = root.resolve("incomplete")
      Files.createDirectory(incomplete): Unit
      Files.writeString(incomplete.resolve("partial"), "keep", StandardCharsets.UTF_8): Unit
      assertEquals(ClusterSnapshotRepository.prune(root, retain = 2), Vector(first))
      assert(!Files.exists(first))
      assert(Files.exists(second) && Files.exists(third) && Files.exists(incomplete))
    finally deleteTree(root)
  }

  test("an invalid published generation fails closed before retention removes anything") {
    val root = Files.createTempDirectory("cascade-cluster-snapshot-retention-invalid")
    try
      val first = createGeneration(root, "generation-1", Instant.parse("2026-10-21T01:00:00Z"), 1)
      val second = createGeneration(root, "generation-2", Instant.parse("2026-10-21T02:00:00Z"), 2)
      Files.writeString(second.resolve("node-1").resolve("state.log"), "tampered", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](ClusterSnapshotRepository.prune(root, retain = 1)): Unit
      assert(Files.exists(first) && Files.exists(second))
    finally deleteTree(root)
  }

  private def createGeneration(root: java.nio.file.Path, name: String, created: Instant, id: Int): java.nio.file.Path =
    val source = root.resolve(s"source-$id")
    Files.createDirectory(source): Unit
    Files.writeString(source.resolve("state.log"), s"state-$id", StandardCharsets.UTF_8): Unit
    val generation = root.resolve(name)
    val node = generation.resolve("node-1")
    val local = BackupCreator.createOnline(source, node, () => created)
    val identity = ClusterSnapshotIdentity(
      UUID.nameUUIDFromBytes(name.getBytes(StandardCharsets.UTF_8)), 1, 1L, id.toLong, "a" * 64
    )
    val artifact = NodeSnapshotArtifact(
      1,
      "11111111-1111-4111-8111-111111111111",
      "node-1",
      Sha256.file(node.resolve(BackupManifest.FileName)),
      local.entries.size,
      local.entries.map(_.length).sum,
      Vector.empty
    )
    ClusterSnapshotManifestCodec.writeAtomic(generation, ClusterSnapshotManifest(identity, created, Vector(artifact)))
    deleteTree(source)
    generation

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.sorted(java.util.Comparator.reverseOrder()).forEach(path => Files.deleteIfExists(path): Unit)
      finally paths.close()

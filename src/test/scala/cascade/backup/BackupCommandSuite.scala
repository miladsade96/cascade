package cascade.backup

import cascade.broker.ShutdownMarker
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters.*

final class BackupCommandSuite extends munit.FunSuite:
  test("runs backup, verification, and restore maintenance commands") {
    val root = Files.createTempDirectory("cascade-backup-command")
    val data = root.resolve("data")
    val backup = root.resolve("backup")
    val restored = root.resolve("restored")
    try
      Files.createDirectories(data): Unit
      Files.writeString(data.resolve("state.log"), "state", StandardCharsets.UTF_8): Unit
      val marker = ShutdownMarker(data)
      marker.beginRecovery()
      marker.markClean()

      val created = BackupCommand.run(Array("backup", "--data-dir", data.toString, "--backup-dir", backup.toString))
      assert(created.startsWith("Created backup with"))
      val verified = BackupCommand.run(Array("verify-backup", "--backup-dir", backup.toString))
      assert(verified.startsWith("Verified"))
      val result = BackupCommand.run(Array("restore", "--backup-dir", backup.toString, "--data-dir", restored.toString))
      assert(result.startsWith("Restored"))
      assertEquals(Files.readString(restored.resolve("state.log"), StandardCharsets.UTF_8), "state")
    finally deleteTree(root)
  }

  test("rejects missing, duplicate, and unknown maintenance options") {
    intercept[IllegalArgumentException](BackupCommand.run(Array("backup", "--data-dir", "data")))
    intercept[IllegalArgumentException](
      BackupCommand.run(Array("verify-backup", "--backup-dir", "one", "--backup-dir", "two"))
    )
    intercept[IllegalArgumentException](BackupCommand.run(Array("backup", "--unknown", "value")))
  }

  test("verifies and prunes collected cluster snapshot generations") {
    val root = Files.createTempDirectory("cascade-cluster-command")
    val source = root.resolve("source")
    val repository = root.resolve("repository")
    try
      Files.createDirectories(source): Unit
      Files.createDirectories(repository): Unit
      Files.writeString(source.resolve("state.log"), "state", StandardCharsets.UTF_8): Unit
      (1 to 2).foreach { index =>
        val generation = repository.resolve(s"generation-$index")
        val node = generation.resolve("node-1")
        val local = BackupCreator.createOnline(source, node)
        val artifact = NodeSnapshotArtifact(
          1,
          "11111111-1111-4111-8111-111111111111",
          "node-1",
          Sha256.file(node.resolve(BackupManifest.FileName)),
          local.entries.size,
          local.entries.map(_.length).sum,
          Vector.empty
        )
        val identity = ClusterSnapshotIdentity(java.util.UUID.randomUUID(), 1, 1L, index.toLong, "a" * 64)
        ClusterSnapshotManifestCodec.writeAtomic(
          generation,
          ClusterSnapshotManifest(identity, java.time.Instant.ofEpochSecond(index.toLong), Vector(artifact))
        )
      }
      val verified = BackupCommand.run(Array(
        "verify-cluster-snapshot", "--snapshot-dir", repository.resolve("generation-2").toString
      ))
      assert(verified.startsWith("Verified cluster snapshot"))
      val pruned = BackupCommand.run(Array(
        "prune-cluster-snapshots", "--repository-dir", repository.toString, "--retain", "1"
      ))
      assert(pruned.startsWith("Removed 1"))
      assert(!Files.exists(repository.resolve("generation-1")))
      assert(Files.exists(repository.resolve("generation-2")))
    finally deleteTree(root)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

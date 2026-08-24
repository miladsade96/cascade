package cascade.backup

import cascade.broker.ShutdownMarker
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters.*

final class BackupRestoreSuite extends munit.FunSuite:
  test("verifies and atomically restores every manifest entry") {
    val root = Files.createTempDirectory("cascade-backup-restore")
    val source = root.resolve("source")
    val backup = root.resolve("backup")
    val restored = root.resolve("restored")
    try
      prepareSource(source)
      val manifest = BackupCreator.create(source, backup)

      val verified = BackupRestore.verify(backup)
      assertEquals(verified.manifest, manifest)
      assertEquals(verified.totalBytes, manifest.entries.map(_.length).sum)

      assertEquals(BackupRestore.restore(backup, restored), verified)
      manifest.entries.foreach { entry =>
        assertEquals(Sha256.file(entry.resolveUnder(restored)), entry.sha256)
      }
      assert(!Files.exists(restored.resolve(BackupManifest.FileName)))
      assert(ShutdownMarker.isCleanlyStopped(restored))
    finally deleteTree(root)
  }

  test("rejects corruption, extra files, and an existing restore target") {
    val root = Files.createTempDirectory("cascade-backup-corrupt")
    val source = root.resolve("source")
    val backup = root.resolve("backup")
    try
      prepareSource(source)
      val manifest = BackupCreator.create(source, backup)
      val first = manifest.entries.find(!_.relativePath.endsWith("clean-shutdown.marker")).get
      Files.writeString(first.resolveUnder(backup), "tampered", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](BackupRestore.verify(backup))

      deleteTree(backup)
      BackupCreator.create(source, backup)
      Files.writeString(backup.resolve("untracked.file"), "extra", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](BackupRestore.verify(backup))

      deleteTree(backup)
      BackupCreator.create(source, backup)
      Files.createDirectory(root.resolve("existing")): Unit
      intercept[IllegalArgumentException](BackupRestore.restore(backup, root.resolve("existing")))
    finally deleteTree(root)
  }

  private def prepareSource(source: java.nio.file.Path): Unit =
    Files.createDirectories(source.resolve("events/0")): Unit
    Files.writeString(source.resolve("events/0/00000000000000000000.log"), "records", StandardCharsets.UTF_8): Unit
    val marker = ShutdownMarker(source)
    marker.beginRecovery()
    marker.markClean()

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

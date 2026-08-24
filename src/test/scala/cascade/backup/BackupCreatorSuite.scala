package cascade.backup

import cascade.broker.ShutdownMarker
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant
import scala.jdk.CollectionConverters.*

final class BackupCreatorSuite extends munit.FunSuite:
  test("publishes an atomic backup of a cleanly stopped broker") {
    val root = Files.createTempDirectory("cascade-backup-create")
    val source = root.resolve("source")
    val target = root.resolve("backup")
    try
      Files.createDirectories(source.resolve("events/0")): Unit
      Files.writeString(source.resolve("events/0/00000000000000000000.log"), "records", StandardCharsets.UTF_8): Unit
      val marker = ShutdownMarker(source)
      marker.beginRecovery()
      marker.markClean()

      val created = BackupCreator.create(source, target, () => Instant.parse("2026-08-24T12:00:00Z"))

      assert(Files.isDirectory(target))
      assertEquals(BackupManifest.read(target.resolve(BackupManifest.FileName)), created)
      assertEquals(
        Files.readString(target.resolve("events/0/00000000000000000000.log"), StandardCharsets.UTF_8),
        "records"
      )
      assert(created.entries.forall(entry => Files.size(entry.resolveUnder(target)) == entry.length))
    finally deleteTree(root)
  }

  test("refuses live brokers, existing targets, and targets inside the source") {
    val root = Files.createTempDirectory("cascade-backup-refusal")
    val source = root.resolve("source")
    val target = root.resolve("backup")
    try
      val marker = ShutdownMarker(source)
      marker.beginRecovery()
      intercept[IllegalArgumentException](BackupCreator.create(source, target))
      marker.markClean()

      Files.createDirectory(target): Unit
      intercept[IllegalArgumentException](BackupCreator.create(source, target))
      intercept[IllegalArgumentException](BackupCreator.create(source, source.resolve("nested-backup")))
    finally deleteTree(root)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

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

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

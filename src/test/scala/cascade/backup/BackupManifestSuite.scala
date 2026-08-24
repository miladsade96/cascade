package cascade.backup

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Instant

final class BackupManifestSuite extends munit.FunSuite:
  test("round-trips a portable sorted manifest") {
    val manifest = BackupManifest(
      Instant.parse("2026-08-24T12:00:00Z"),
      Vector(
        BackupEntry("topics/events/0/000.log", 4L, "a" * 64),
        BackupEntry(".cascade/clean-shutdown.marker", 2L, "b" * 64)
      )
    )

    val encoded = BackupManifest.encode(manifest)
    assert(encoded.indexOf(".cascade") < 0, "paths must be encoded rather than parsed as raw manifest fields")
    assertEquals(BackupManifest.decode(encoded), manifest.copy(entries = manifest.entries.sortBy(_.relativePath)))
  }

  test("rejects traversal, duplicate paths, and malformed checksums") {
    intercept[IllegalArgumentException](BackupEntry("../secret", 1L, "a" * 64))
    intercept[IllegalArgumentException](BackupEntry("C:/secret", 1L, "a" * 64))
    intercept[IllegalArgumentException](BackupEntry("valid", 1L, "invalid"))
    val entry = BackupEntry("valid", 1L, "a" * 64)
    intercept[IllegalArgumentException](BackupManifest(Instant.EPOCH, Vector(entry, entry)))
  }

  test("calculates stable SHA-256 values") {
    val file = Files.createTempFile("cascade-sha256", ".txt")
    try
      Files.writeString(file, "cascade", StandardCharsets.UTF_8)
      assertEquals(Sha256.file(file), "fd782ad6a7d31feedc1ea128c85526be3cad24a27ff92ef24f562eb52ac5dba2")
    finally Files.deleteIfExists(file): Unit
  }

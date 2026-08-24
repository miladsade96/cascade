package cascade.backup

import java.io.InputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.security.MessageDigest
import java.time.Instant
import java.util.Base64

final case class BackupEntry(relativePath: String, length: Long, sha256: String):
  require(BackupEntry.isSafe(relativePath), s"unsafe backup path: $relativePath")
  require(length >= 0L, "backup entry length cannot be negative")
  require(sha256.matches("[0-9a-f]{64}"), "backup entry SHA-256 must be lowercase hexadecimal")

  def resolveUnder(root: Path): Path =
    val normalizedRoot = root.toAbsolutePath.normalize()
    val resolved = relativePath.split('/').foldLeft(normalizedRoot)((current, name) => current.resolve(name)).normalize()
    require(resolved.startsWith(normalizedRoot), s"backup path escapes destination: $relativePath")
    resolved

object BackupEntry:
  private def isSafe(value: String): Boolean =
    value.nonEmpty &&
      !value.startsWith("/") &&
      !value.contains('\\') &&
      !value.contains(':') &&
      !value.contains('\u0000') &&
      value.split("/", -1).forall(segment => segment.nonEmpty && segment != "." && segment != "..")

final case class BackupManifest(createdAt: Instant, entries: Vector[BackupEntry]):
  require(entries.map(_.relativePath).distinct.size == entries.size, "backup manifest paths must be unique")

object BackupManifest:
  val FileName = "cascade-backup.manifest"
  private val Header = "CASCADE-BACKUP\t1"

  def encode(manifest: BackupManifest): String =
    val body = manifest.entries.sortBy(_.relativePath).map { entry =>
      val path = Base64.getUrlEncoder.withoutPadding().encodeToString(entry.relativePath.getBytes(StandardCharsets.UTF_8))
      s"file\t$path\t${entry.length}\t${entry.sha256}"
    }
    (Vector(Header, s"created_at\t${manifest.createdAt}", s"file_count\t${manifest.entries.size}") ++ body).mkString("", "\n", "\n")

  def decode(value: String): BackupManifest =
    val lines = value.linesIterator.toVector
    require(lines.length >= 3, "backup manifest is incomplete")
    require(lines.head == Header, "unsupported backup manifest format")
    val created = lines(1).split("\\t", -1).toVector match
      case Vector("created_at", timestamp) => Instant.parse(timestamp)
      case _ => throw IllegalArgumentException("backup manifest creation time is invalid")
    val count = lines(2).split("\\t", -1).toVector match
      case Vector("file_count", number) => number.toInt
      case _ => throw IllegalArgumentException("backup manifest file count is invalid")
    require(count >= 0 && lines.length == count + 3, "backup manifest file count does not match its entries")
    val entries = lines.drop(3).map { line =>
      line.split("\\t", -1).toVector match
        case Vector("file", encodedPath, length, checksum) =>
          val relativePath = String(Base64.getUrlDecoder.decode(encodedPath), StandardCharsets.UTF_8)
          BackupEntry(relativePath, length.toLong, checksum)
        case _ => throw IllegalArgumentException("backup manifest entry is invalid")
    }
    BackupManifest(created, entries)

  def read(path: Path): BackupManifest = decode(Files.readString(path, StandardCharsets.UTF_8))

object Sha256:
  def file(path: Path): String =
    val input = Files.newInputStream(path)
    try stream(input)
    finally input.close()

  def stream(input: InputStream): String =
    val digest = MessageDigest.getInstance("SHA-256")
    val buffer = new Array[Byte](1024 * 1024)
    var read = input.read(buffer)
    while read >= 0 do
      if read > 0 then digest.update(buffer, 0, read)
      read = input.read(buffer)
    digest.digest().map(byte => f"${byte & 0xff}%02x").mkString

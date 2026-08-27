package cascade.security

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption.{APPEND, CREATE, WRITE}
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

final case class AuditEvent(
    eventType: String,
    principal: String,
    remoteAddress: String,
    secure: Boolean,
    decision: String,
    operation: Option[String] = None,
    resourceType: Option[String] = None,
    resource: Option[String] = None,
    mechanism: Option[String] = None
)

final class AuditLog private (channel: FileChannel, forceEachEvent: Boolean) extends AutoCloseable:
  private val closed = AtomicBoolean(false)

  def record(event: AuditEvent): Unit = synchronized {
    if closed.get() then return
    val fields = Vector(
      "timestamp" -> Instant.now().toString,
      "event" -> event.eventType,
      "principal" -> event.principal,
      "remote_address" -> event.remoteAddress,
      "secure" -> event.secure.toString,
      "decision" -> event.decision
    ) ++ event.operation.map("operation" -> _) ++
      event.resourceType.map("resource_type" -> _) ++ event.resource.map("resource" -> _) ++
      event.mechanism.map("mechanism" -> _)
    val json = fields.map { case (key, value) => s"\"${escape(key)}\":\"${escape(value)}\"" }.mkString("{", ",", "}\n")
    val bytes = ByteBuffer.wrap(json.getBytes(StandardCharsets.UTF_8))
    while bytes.hasRemaining do channel.write(bytes): Unit
    if forceEachEvent then channel.force(false)
  }

  override def close(): Unit = synchronized {
    if closed.compareAndSet(false, true) then
      channel.force(false)
      channel.close()
  }

  private def escape(value: String): String =
    val result = StringBuilder(value.length + 16)
    value.foreach {
      case '"'  => result.append("\\\"")
      case '\\' => result.append("\\\\")
      case '\b' => result.append("\\b")
      case '\f' => result.append("\\f")
      case '\n' => result.append("\\n")
      case '\r' => result.append("\\r")
      case '\t' => result.append("\\t")
      case character if character < ' ' => result.append(f"\\u${character.toInt}%04x")
      case character => result.append(character)
    }
    result.result()

object AuditLog:
  def open(path: Path, forceEachEvent: Boolean): AuditLog =
    Option(path.toAbsolutePath.getParent).foreach(parent => Files.createDirectories(parent): Unit)
    AuditLog(FileChannel.open(path, CREATE, WRITE, APPEND), forceEachEvent)

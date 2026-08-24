package cascade.operations

import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.StandardOpenOption.{APPEND, CREATE, WRITE}
import java.nio.file.{Files, Path}
import java.time.Instant
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.util.control.NonFatal

enum EventLevel:
  case Info, Warn, Error

final class StructuredLogger(
    path: Option[Path],
    maxBytes: Long,
    retainedFiles: Int,
    stderrEnabled: Boolean,
    clock: () => Instant = () => Instant.now(),
    stderr: PrintStream = System.err
) extends AutoCloseable:
  require(maxBytes >= 1024L, "structured log size must be at least 1 KiB")
  require(retainedFiles > 0, "structured log retention must be positive")

  private val closed = AtomicBoolean(false)
  private val failure = AtomicReference(Option.empty[String])
  private var channel = path.map(open)
  private var size = path.filter(Files.exists(_)).map(Files.size).getOrElse(0L)

  def info(event: String, fields: Map[String, String] = Map.empty): Unit = record(EventLevel.Info, event, fields)

  def warn(event: String, fields: Map[String, String] = Map.empty): Unit = record(EventLevel.Warn, event, fields)

  def error(event: String, throwable: Throwable, fields: Map[String, String] = Map.empty): Unit =
    record(
      EventLevel.Error,
      event,
      fields ++ Map(
        "error_type" -> throwable.getClass.getName,
        "error_message" -> Option(throwable.getMessage).getOrElse(throwable.getClass.getSimpleName)
      )
    )

  def lastFailure: Option[String] = failure.get()

  def record(level: EventLevel, event: String, fields: Map[String, String] = Map.empty): Unit = synchronized {
    if closed.get() then return
    val line = encode(level, event, fields)
    if stderrEnabled then stderr.print(line)
    channel.foreach { current =>
      try
        val bytes = line.getBytes(StandardCharsets.UTF_8)
        if size > 0L && size + bytes.length > maxBytes then rotate()
        val target = channel.getOrElse(current)
        val buffer = ByteBuffer.wrap(bytes)
        while buffer.hasRemaining do target.write(buffer): Unit
        size += bytes.length
        failure.set(None)
      catch
        case NonFatal(error) =>
          failure.set(Some(Option(error.getMessage).getOrElse(error.getClass.getSimpleName)))
          if !stderrEnabled then stderr.println(encode(EventLevel.Error, "structured_log_failure", Map("message" -> failure.get().get)))
    }
  }

  override def close(): Unit = synchronized {
    if closed.compareAndSet(false, true) then
      channel.foreach { current =>
        current.force(false)
        current.close()
      }
      channel = None
  }

  private def rotate(): Unit =
    channel.foreach(_.close())
    channel = None
    path.foreach { current =>
      (retainedFiles to 2 by -1).foreach { index =>
        val previous = rotated(current, index - 1)
        if Files.exists(previous) then Files.move(previous, rotated(current, index), REPLACE_EXISTING): Unit
      }
      if Files.exists(current) then Files.move(current, rotated(current, 1), REPLACE_EXISTING): Unit
      channel = Some(open(current))
      size = 0L
    }

  private def open(file: Path): FileChannel =
    Option(file.toAbsolutePath.getParent).foreach(parent => Files.createDirectories(parent): Unit)
    FileChannel.open(file, CREATE, WRITE, APPEND)

  private def rotated(file: Path, index: Int): Path = file.resolveSibling(s"${file.getFileName}.$index")

  private def encode(level: EventLevel, event: String, fields: Map[String, String]): String =
    val base = Vector(
      "timestamp" -> clock().toString,
      "level" -> level.toString.toLowerCase,
      "event" -> event
    )
    (base ++ fields.toVector.sortBy(_._1))
      .map { case (key, value) => s"\"${escape(key)}\":\"${escape(value)}\"" }
      .mkString("{", ",", "}\n")

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

object StructuredLogger:
  def from(config: OperationsConfig): StructuredLogger =
    StructuredLogger(
      config.structuredLog,
      config.structuredLogMaxBytes,
      config.structuredLogRetainedFiles,
      config.logToStderr
    )

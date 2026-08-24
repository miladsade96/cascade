package cascade.operations

import cascade.security.SecurityTestSupport
import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.Files
import java.time.Instant
import munit.FunSuite

final class StructuredLoggerSuite extends FunSuite:
  test("writes escaped deterministic JSON events to file and stderr") {
    val directory = Files.createTempDirectory("cascade-structured-log")
    val path = directory.resolve("broker.jsonl")
    val stderrBytes = ByteArrayOutputStream()
    val logger = StructuredLogger(
      Some(path),
      maxBytes = 4096L,
      retainedFiles = 2,
      stderrEnabled = true,
      clock = () => Instant.parse("2026-08-24T12:00:00Z"),
      stderr = PrintStream(stderrBytes)
    )
    try
      logger.warn("capacity\nalert", Map("topic" -> "orders\"private"))
      val line = Files.readString(path)
      assertEquals(line, String(stderrBytes.toByteArray))
      assert(line.contains("\"timestamp\":\"2026-08-24T12:00:00Z\""))
      assert(line.contains("\"event\":\"capacity\\nalert\""))
      assert(line.contains("orders\\\"private"))
    finally
      logger.close()
      SecurityTestSupport.deleteTree(directory)
  }

  test("rotates bounded files and keeps the requested generations") {
    val directory = Files.createTempDirectory("cascade-structured-rotation")
    val path = directory.resolve("broker.jsonl")
    val logger = StructuredLogger(Some(path), 1024L, 2, stderrEnabled = false)
    try
      (0 until 40).foreach(index => logger.info("large_event", Map("index" -> index.toString, "payload" -> ("x" * 80))))
      assert(Files.exists(path))
      assert(Files.exists(path.resolveSibling("broker.jsonl.1")))
      assert(Files.exists(path.resolveSibling("broker.jsonl.2")))
      assert(!Files.exists(path.resolveSibling("broker.jsonl.3")))
      assertEquals(logger.lastFailure, None)
    finally
      logger.close()
      SecurityTestSupport.deleteTree(directory)
  }

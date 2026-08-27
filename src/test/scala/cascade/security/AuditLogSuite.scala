package cascade.security

import java.nio.file.Files
import munit.FunSuite

final class AuditLogSuite extends FunSuite:
  test("writes durable JSON lines without allowing field injection") {
    val directory = Files.createTempDirectory("cascade-audit")
    val path = directory.resolve("security.jsonl")
    val audit = AuditLog.open(path, forceEachEvent = true)
    try
      audit.record(
        AuditEvent(
          "authorization",
          "alice\"\nadmin",
          "127.0.0.1",
          secure = true,
          "denied",
          Some("Write"),
          Some("Topic"),
          Some("private"),
          Some("SCRAM-SHA-256")
        )
      )
      val content = Files.readString(path)
      assert(content.endsWith("\n"))
      assert(content.contains("\"event\":\"authorization\""))
      assert(content.contains("alice\\\"\\nadmin"))
      assert(content.contains("\"mechanism\":\"SCRAM-SHA-256\""))
      assertEquals(content.lines().count(), 1L)
    finally
      audit.close()
      SecurityTestSupport.deleteTree(directory)
  }

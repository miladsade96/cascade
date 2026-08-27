package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Arrays

final class ReloadableScramCredentialsSuite extends munit.FunSuite:
  test("rotates SCRAM verifiers atomically and preserves the last valid snapshot") {
    val path = Files.createTempFile("cascade-reloadable-scram", ".conf")
    val first = "first-password".toCharArray
    val second = "second-password".toCharArray
    try
      write(path, first, SaslMechanism.ScramSha256)
      val credentials = ReloadableScramCredentials(path, reloadIntervalMillis = 0L)
      val original = credentials.credential(SaslMechanism.ScramSha256, "alice").get

      write(path, second, SaslMechanism.ScramSha512)
      assert(credentials.credential(SaslMechanism.ScramSha512, "alice").nonEmpty)
      assertEquals(credentials.credential(SaslMechanism.ScramSha256, "alice"), None)

      Files.writeString(path, "malformed", StandardCharsets.UTF_8): Unit
      assert(credentials.credential(SaslMechanism.ScramSha512, "alice").nonEmpty)
      assert(credentials.lastReloadError.nonEmpty)

      write(path, first, SaslMechanism.ScramSha256)
      assert(credentials.reloadNow())
      assertEquals(credentials.lastReloadError, None)
      assert(credentials.credential(SaslMechanism.ScramSha256, "alice").exists(_.iterations == original.iterations))
    finally
      Arrays.fill(first, '\u0000')
      Arrays.fill(second, '\u0000')
      Files.deleteIfExists(path): Unit
  }

  private def write(path: java.nio.file.Path, password: Array[Char], mechanism: SaslMechanism): Unit =
    val line = CredentialTool.generateScramLine("alice", password, mechanism, ScramCredential.MinimumIterations)
    Files.writeString(path, line + "\n", StandardCharsets.UTF_8): Unit

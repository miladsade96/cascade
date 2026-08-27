package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Arrays

final class ScramCredentialFileSuite extends munit.FunSuite:
  test("loads independent SHA-256 and SHA-512 verifiers for one principal") {
    val path = Files.createTempFile("cascade-scram-credentials", ".conf")
    val password = "scram-file-password".toCharArray
    try
      val sha256 = ScramCredential.create(SaslMechanism.ScramSha256, password, ScramCredential.MinimumIterations)
      val sha512 = ScramCredential.create(SaslMechanism.ScramSha512, password, ScramCredential.MinimumIterations)
      Files.writeString(
        path,
        Vector(
          "# generated verifier set",
          ScramCredentialFile.encode(SaslMechanism.ScramSha256, "alice", sha256),
          ScramCredentialFile.encode(SaslMechanism.ScramSha512, "alice", sha512)
        ).mkString("\n"),
        StandardCharsets.UTF_8
      ): Unit

      val loaded = ScramCredentialFile.load(path)
      assertEquals(loaded.principals, Set("alice"))
      assertEquals(loaded.mechanisms, Set(SaslMechanism.ScramSha256, SaslMechanism.ScramSha512))
      assertEquals(loaded.credential(SaslMechanism.ScramSha256, "alice").map(_.iterations), Some(4096))
      assertEquals(loaded.credential(SaslMechanism.ScramSha512, "alice").map(_.serverKey.length), Some(64))
    finally
      Arrays.fill(password, '\u0000')
      Files.deleteIfExists(path): Unit
  }

  test("rejects duplicate, weak, mismatched, and malformed verifier records") {
    val path = Files.createTempFile("cascade-invalid-scram", ".conf")
    val password = "password".toCharArray
    try
      val credential = ScramCredential.create(SaslMechanism.ScramSha256, password, ScramCredential.MinimumIterations)
      val valid = ScramCredentialFile.encode(SaslMechanism.ScramSha256, "alice", credential)
      Files.writeString(path, s"$valid\n$valid\n", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](ScramCredentialFile.load(path))

      Files.writeString(path, valid.replace("scram-sha-256", "scram-sha-512"), StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](ScramCredentialFile.load(path))

      Files.writeString(path, "PLAIN alice=not-scram", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](ScramCredentialFile.load(path))

      Files.writeString(path, "malformed", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](ScramCredentialFile.load(path))
    finally
      Arrays.fill(password, '\u0000')
      Files.deleteIfExists(path): Unit
  }

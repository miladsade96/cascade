package cascade.security

import java.nio.file.Files
import java.util.Arrays
import munit.FunSuite

final class CredentialFileSuite extends FunSuite:
  test("loads salted PBKDF2 credentials and verifies passwords in constant time") {
    val path = Files.createTempFile("cascade-credentials", ".conf")
    val password = "correct horse battery staple".toCharArray
    try
      val encoded = CredentialHash.create(password, CredentialHash.MinimumIterations)
      Files.writeString(path, s"# generated for the test\nalice=$encoded\n")
      val credentials = CredentialFile.load(path)

      assert(credentials("alice").verify(password))
      assert(!credentials("alice").verify("wrong".toCharArray))
      assert(encoded.startsWith("pbkdf2-sha256$"))
      assert(!encoded.contains(String(password)))
    finally
      Arrays.fill(password, '\u0000')
      Files.deleteIfExists(path): Unit
  }

  test("rejects duplicate and weak credential records") {
    val path = Files.createTempFile("cascade-invalid-credentials", ".conf")
    try
      Files.writeString(path, "alice=pbkdf2-sha256$1$AAAAAAAAAAAAAAAAAAAAAA==$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\n")
      intercept[IllegalArgumentException](CredentialFile.load(path))
      Files.writeString(path, "alice=invalid\n")
      intercept[IllegalArgumentException](CredentialFile.load(path))
    finally Files.deleteIfExists(path): Unit
  }

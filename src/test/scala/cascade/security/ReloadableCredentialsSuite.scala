package cascade.security

import java.nio.file.Files
import java.util.Arrays
import munit.FunSuite

final class ReloadableCredentialsSuite extends FunSuite:
  test("rotates credentials atomically and keeps the last valid snapshot") {
    val path = Files.createTempFile("cascade-reloadable-credentials", ".conf")
    val first = "first-password".toCharArray
    val second = "second-password".toCharArray
    try
      Files.writeString(path, s"alice=${CredentialHash.create(first, CredentialHash.MinimumIterations)}\n")
      val store = ReloadableCredentials(path, reloadIntervalMillis = 60_000L)
      assert(store.authenticate("alice", first))

      Files.writeString(path, s"alice=${CredentialHash.create(second, CredentialHash.MinimumIterations)}\n")
      assert(store.reloadNow())
      assert(!store.authenticate("alice", first))
      assert(store.authenticate("alice", second))

      Files.writeString(path, "alice=broken\n")
      assert(!store.reloadNow())
      assert(store.lastReloadError.nonEmpty)
      assert(store.authenticate("alice", second))
    finally
      Arrays.fill(first, '\u0000')
      Arrays.fill(second, '\u0000')
      Files.deleteIfExists(path): Unit
  }

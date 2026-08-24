package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.Files

final class ReloadablePeerIdentitiesSuite extends munit.FunSuite:
  test("rotates node identities atomically and preserves the last valid policy") {
    val file = Files.createTempFile("cascade-reloadable-peer-identities", ".conf")
    try
      Files.writeString(file, "1 CN=broker-old,O=Cascade\n", StandardCharsets.UTF_8): Unit
      val identities = ReloadablePeerIdentities(file, reloadIntervalMillis = 0L)
      assert(identities.authorize(1, "CN=broker-old,O=Cascade"))

      Files.writeString(file, "1 CN=broker-new,O=Cascade\n", StandardCharsets.UTF_8): Unit
      assert(identities.authorize(1, "CN=broker-new,O=Cascade"))
      assert(!identities.authorize(1, "CN=broker-old,O=Cascade"))

      Files.writeString(file, "malformed", StandardCharsets.UTF_8): Unit
      assert(identities.lastReloadError.nonEmpty)
      assert(identities.authorize(1, "CN=broker-new,O=Cascade"))
      assert(identities.lastReloadError.nonEmpty)

      Files.writeString(file, "1 CN=broker-final,O=Cascade\n", StandardCharsets.UTF_8): Unit
      assert(identities.reloadNow())
      assert(identities.authorize(1, "CN=broker-final,O=Cascade"))
      assertEquals(identities.lastReloadError, None)
    finally Files.deleteIfExists(file): Unit
  }

package cascade.security

import java.nio.file.{Files, StandardCopyOption}
import munit.FunSuite

final class ReloadableTlsContextSuite extends FunSuite:
  test("installs a changed key store as one new TLS generation") {
    val directory = Files.createTempDirectory("cascade-reloadable-tls")
    try
      val first = SecurityTestSupport.createKeyStore(directory, "first.p12")
      val second = SecurityTestSupport.createKeyStore(directory, "second.p12")
      val active = directory.resolve("active.p12")
      Files.copy(first, active)
      val reloader = ReloadableTlsContext(
        TlsConfig(
          keyStore = Some(active),
          keyStorePassword = Some(SecurityTestSupport.StorePassword),
          reloadIntervalMillis = 0L
        )
      )
      try
        assertEquals(reloader.current.generation, 0L)
        assert(!reloader.reloadNow())

        Files.copy(second, active, StandardCopyOption.REPLACE_EXISTING)
        assert(reloader.reloadNow())
        assertEquals(reloader.current.generation, 1L)
        assertEquals(reloader.snapshot.successfulReloads, 1L)
        assertEquals(reloader.snapshot.failedReloads, 0L)
        assertEquals(reloader.lastReloadError, None)
        assert(!reloader.reloadNow())
        assertEquals(reloader.current.generation, 1L)
      finally reloader.close()
    finally SecurityTestSupport.deleteTree(directory)
  }

package cascade.security

import java.nio.file.{Files, StandardCopyOption}
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicInteger
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

  test("keeps the last valid context and deduplicates an unchanged invalid replacement") {
    val directory = Files.createTempDirectory("cascade-reloadable-tls-invalid")
    val failures = AtomicInteger(0)
    try
      val valid = SecurityTestSupport.createKeyStore(directory, "valid.p12")
      val replacement = SecurityTestSupport.createKeyStore(directory, "replacement.p12")
      val active = directory.resolve("active.p12")
      Files.copy(valid, active)
      val reloader = ReloadableTlsContext(
        TlsConfig(
          keyStore = Some(active),
          keyStorePassword = Some(SecurityTestSupport.StorePassword),
          reloadIntervalMillis = 0L
        ),
        onFailure = _ => failures.incrementAndGet(): Unit
      )
      try
        Files.writeString(active, "not a key store", StandardCharsets.UTF_8)
        assert(!reloader.reloadNow())
        assertEquals(reloader.current.generation, 0L)
        assertEquals(reloader.snapshot.failedReloads, 1L)
        assertEquals(failures.get(), 1)
        assert(reloader.lastReloadError.nonEmpty)

        assert(!reloader.reloadNow())
        assertEquals(reloader.snapshot.failedReloads, 1L)
        assertEquals(failures.get(), 1)

        Files.copy(valid, active, StandardCopyOption.REPLACE_EXISTING)
        assert(!reloader.reloadNow())
        assertEquals(reloader.current.generation, 0L)
        assertEquals(reloader.lastReloadError, None)

        Files.copy(replacement, active, StandardCopyOption.REPLACE_EXISTING)
        assert(reloader.reloadNow())
        assertEquals(reloader.current.generation, 1L)
      finally reloader.close()
    finally SecurityTestSupport.deleteTree(directory)
  }

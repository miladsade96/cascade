package cascade.security

import java.io.RandomAccessFile
import java.nio.file.Files
import javax.net.ssl.SSLServerSocket
import munit.FunSuite

final class TlsContextFactorySuite extends FunSuite:
  test("builds a server context from a PKCS12 key store") {
    val directory = Files.createTempDirectory("cascade-tls-context")
    try
      val keyStore = SecurityTestSupport.createKeyStore(directory)
      val context = TlsContextFactory.create(
        TlsConfig(keyStore = Some(keyStore), keyStorePassword = Some(SecurityTestSupport.StorePassword))
      )
      val socket = context.getServerSocketFactory.createServerSocket().asInstanceOf[SSLServerSocket]
      try assert(socket.getSupportedProtocols.contains("TLSv1.3"))
      finally socket.close()
    finally SecurityTestSupport.deleteTree(directory)
  }

  test("rejects empty and oversized TLS stores before parsing them") {
    val directory = Files.createTempDirectory("cascade-tls-context-bounds")
    try
      val empty = Files.createFile(directory.resolve("empty.p12"))
      intercept[IllegalArgumentException] {
        TlsContextFactory.create(TlsConfig(keyStore = Some(empty), keyStorePassword = Some("unused")))
      }

      val oversized = directory.resolve("oversized.p12")
      val file = RandomAccessFile(oversized.toFile, "rw")
      try file.setLength(64L * 1024L * 1024L + 1L)
      finally file.close()
      val error = intercept[IllegalArgumentException] {
        TlsContextFactory.create(TlsConfig(keyStore = Some(oversized), keyStorePassword = Some("unused")))
      }
      assert(error.getMessage.contains("exceeds"))
    finally SecurityTestSupport.deleteTree(directory)
  }

  test("fails closed when a TLS store password is wrong") {
    val directory = Files.createTempDirectory("cascade-tls-context-password")
    try
      val keyStore = SecurityTestSupport.createKeyStore(directory)
      intercept[Exception] {
        TlsContextFactory.create(TlsConfig(keyStore = Some(keyStore), keyStorePassword = Some("wrong-password")))
      }
    finally SecurityTestSupport.deleteTree(directory)
  }

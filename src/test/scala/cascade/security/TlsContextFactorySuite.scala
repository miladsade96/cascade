package cascade.security

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

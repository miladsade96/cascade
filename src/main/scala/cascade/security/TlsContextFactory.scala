package cascade.security

import java.io.InputStream
import java.nio.file.{Files, Path}
import java.security.KeyStore
import java.util.Arrays
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

object TlsContextFactory:
  def create(config: TlsConfig): SSLContext =
    val keyStorePath = config.keyStore.getOrElse(throw IllegalArgumentException("TLS requires a key store"))
    val storePassword = config.keyStorePassword.getOrElse(throw IllegalArgumentException("TLS requires a key-store password"))
    val keyPassword = config.keyPassword.getOrElse(storePassword)
    val keyStore = loadStore(keyStorePath, storePassword)
    val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    val keyPasswordChars = keyPassword.toCharArray
    try keyManagers.init(keyStore, keyPasswordChars)
    finally Arrays.fill(keyPasswordChars, '\u0000')

    val trustManagers = config.trustStore.map { path =>
      val password = config.trustStorePassword.getOrElse(throw IllegalArgumentException("TLS trust store requires a password"))
      val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      factory.init(loadStore(path, password))
      factory.getTrustManagers
    }.orNull

    val context = SSLContext.getInstance("TLS")
    context.init(keyManagers.getKeyManagers, trustManagers, null)
    context

  private def loadStore(path: Path, password: String): KeyStore =
    val fileName = path.getFileName.toString.toLowerCase
    val storeType = if fileName.endsWith(".jks") then "JKS" else "PKCS12"
    val store = KeyStore.getInstance(storeType)
    val passwordChars = password.toCharArray
    var input: InputStream | Null = null
    try
      input = Files.newInputStream(path)
      store.load(input, passwordChars)
      store
    finally
      if input != null then input.close()
      Arrays.fill(passwordChars, '\u0000')

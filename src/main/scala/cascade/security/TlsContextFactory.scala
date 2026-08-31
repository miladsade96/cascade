package cascade.security

import java.io.ByteArrayInputStream
import java.nio.file.{Files, Path}
import java.security.{KeyStore, MessageDigest}
import java.util.{Arrays, HexFormat}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

object TlsContextFactory:
  private val MaximumStoreBytes = 64L * 1024L * 1024L

  def create(config: TlsConfig): SSLContext =
    val material = readMaterial(config)
    try material.createContext()
    finally material.close()

  private[security] def readMaterial(config: TlsConfig): TlsMaterial =
    val keyStorePath = config.keyStore.getOrElse(throw IllegalArgumentException("TLS requires a key store"))
    val keyStoreBytes = readStore(keyStorePath)
    try
      val trustStoreBytes = config.trustStore.map(readStore)
      TlsMaterial(config, keyStoreBytes, trustStoreBytes)
    catch
      case error: Throwable =>
        Arrays.fill(keyStoreBytes, 0.toByte)
        throw error

  private def readStore(path: Path): Array[Byte] =
    val size = Files.size(path)
    if size <= 0L then throw IllegalArgumentException(s"TLS store is empty: $path")
    if size > MaximumStoreBytes then
      throw IllegalArgumentException(s"TLS store exceeds the $MaximumStoreBytes byte limit: $path")
    val bytes = Files.readAllBytes(path)
    if bytes.length.toLong > MaximumStoreBytes then
      Arrays.fill(bytes, 0.toByte)
      throw IllegalArgumentException(s"TLS store exceeds the $MaximumStoreBytes byte limit: $path")
    bytes

private[security] final class TlsMaterial private (
    config: TlsConfig,
    keyStoreBytes: Array[Byte],
    trustStoreBytes: Option[Array[Byte]]
) extends AutoCloseable:
  val fingerprint: String =
    val digest = MessageDigest.getInstance("SHA-256")
    digest.update(0.toByte)
    digest.update(keyStoreBytes)
    digest.update(1.toByte)
    trustStoreBytes.foreach(digest.update)
    HexFormat.of().formatHex(digest.digest())

  def createContext(): SSLContext =
    val keyStorePath = config.keyStore.getOrElse(throw IllegalArgumentException("TLS requires a key store"))
    val storePassword = config.keyStorePassword.getOrElse(throw IllegalArgumentException("TLS requires a key-store password"))
    val keyPassword = config.keyPassword.getOrElse(storePassword)
    val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    val keyPasswordChars = keyPassword.toCharArray
    try keyManagers.init(loadStore(keyStorePath, keyStoreBytes, storePassword), keyPasswordChars)
    finally Arrays.fill(keyPasswordChars, '\u0000')

    val trustManagers = config.trustStore.map { path =>
      val password = config.trustStorePassword.getOrElse(throw IllegalArgumentException("TLS trust store requires a password"))
      val bytes = trustStoreBytes.getOrElse(throw IllegalStateException("TLS trust-store bytes are unavailable"))
      val factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
      factory.init(loadStore(path, bytes, password))
      factory.getTrustManagers
    }.orNull

    val context = SSLContext.getInstance("TLS")
    context.init(keyManagers.getKeyManagers, trustManagers, null)
    context

  override def close(): Unit =
    Arrays.fill(keyStoreBytes, 0.toByte)
    trustStoreBytes.foreach(bytes => Arrays.fill(bytes, 0.toByte))

  private def loadStore(path: Path, bytes: Array[Byte], password: String): KeyStore =
    val fileName = path.getFileName.toString.toLowerCase
    val storeType = if fileName.endsWith(".jks") then "JKS" else "PKCS12"
    val store = KeyStore.getInstance(storeType)
    val passwordChars = password.toCharArray
    val input = ByteArrayInputStream(bytes)
    try
      store.load(input, passwordChars)
      store
    finally
      input.close()
      Arrays.fill(passwordChars, '\u0000')

private[security] object TlsMaterial:
  def apply(config: TlsConfig, keyStoreBytes: Array[Byte], trustStoreBytes: Option[Array[Byte]]): TlsMaterial =
    new TlsMaterial(config, keyStoreBytes, trustStoreBytes)

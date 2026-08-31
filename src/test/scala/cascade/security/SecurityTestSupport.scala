package cascade.security

import java.security.KeyStore
import java.nio.file.{Files, Path}
import java.util.Arrays

object SecurityTestSupport:
  val StorePassword = "cascade-test-password"

  final case class MutualTlsMaterial(
      keyStores: Map[Int, Path],
      trustStore: Path,
      principals: Map[Int, String]
  )

  def createKeyStore(
      directory: Path,
      name: String = "broker.p12",
      subjectAlternativeNames: String = "dns:localhost,ip:127.0.0.1"
  ): Path =
    val path = directory.resolve(name)
    runKeytool(
      "-genkeypair",
      "-alias", "cascade",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "2",
      "-dname", "CN=localhost, OU=Test, O=Cascade, L=Test, ST=Test, C=US",
      "-ext", s"SAN=$subjectAlternativeNames",
      "-storetype", "PKCS12",
      "-keystore", path.toString,
      "-storepass", StorePassword,
      "-keypass", StorePassword,
      "-noprompt"
    )
    path

  def createMutualTlsMaterial(directory: Path, nodeIds: Vector[Int]): MutualTlsMaterial =
    require(nodeIds.nonEmpty && nodeIds.distinct.size == nodeIds.size, "mutual TLS node IDs must be non-empty and unique")
    val caStore = directory.resolve("ca.p12")
    val caCertificate = directory.resolve("ca.cer")
    val trustStore = directory.resolve("trust.p12")
    runKeytool(
      "-genkeypair", "-alias", "ca", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
      "-dname", "CN=Cascade Test CA, O=Cascade", "-ext", "bc=ca:true", "-storetype", "PKCS12",
      "-keystore", caStore.toString, "-storepass", StorePassword, "-keypass", StorePassword, "-noprompt"
    )
    runKeytool(
      "-exportcert", "-rfc", "-alias", "ca", "-keystore", caStore.toString,
      "-storepass", StorePassword, "-file", caCertificate.toString
    )
    runKeytool(
      "-importcert", "-alias", "ca", "-file", caCertificate.toString, "-storetype", "PKCS12",
      "-keystore", trustStore.toString, "-storepass", StorePassword, "-noprompt"
    )
    val keyStores = nodeIds.map { nodeId =>
      val alias = s"broker-$nodeId"
      val keyStore = directory.resolve(s"$alias.p12")
      val request = directory.resolve(s"$alias.csr")
      val certificate = directory.resolve(s"$alias.cer")
      val distinguishedName = s"CN=$alias, OU=Test, O=Cascade"
      runKeytool(
        "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-validity", "2",
        "-dname", distinguishedName, "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-storetype", "PKCS12",
        "-keystore", keyStore.toString, "-storepass", StorePassword, "-keypass", StorePassword, "-noprompt"
      )
      runKeytool(
        "-certreq", "-alias", alias, "-keystore", keyStore.toString, "-storepass", StorePassword,
        "-file", request.toString, "-ext", "SAN=dns:localhost,ip:127.0.0.1"
      )
      runKeytool(
        "-gencert", "-rfc", "-alias", "ca", "-keystore", caStore.toString, "-storepass", StorePassword,
        "-infile", request.toString, "-outfile", certificate.toString, "-validity", "2",
        "-ext", "KU=digitalSignature,keyEncipherment", "-ext", "EKU=serverAuth,clientAuth",
        "-ext", "SAN=dns:localhost,ip:127.0.0.1"
      )
      runKeytool(
        "-importcert", "-alias", "ca", "-file", caCertificate.toString, "-keystore", keyStore.toString,
        "-storepass", StorePassword, "-noprompt"
      )
      runKeytool(
        "-importcert", "-alias", alias, "-file", certificate.toString, "-keystore", keyStore.toString,
        "-storepass", StorePassword, "-noprompt"
      )
      nodeId -> keyStore
    }.toMap
    MutualTlsMaterial(
      keyStores,
      trustStore,
      nodeIds.map(nodeId => nodeId -> s"CN=broker-$nodeId,OU=Test,O=Cascade").toMap
    )

  def combineTrustStores(directory: Path, name: String, sources: Vector[Path]): Path =
    require(sources.nonEmpty, "at least one trust store is required")
    val password = StorePassword.toCharArray
    val combined = KeyStore.getInstance("PKCS12")
    try
      combined.load(null, password)
      sources.zipWithIndex.foreach { case (source, index) =>
        val inputStore = KeyStore.getInstance("PKCS12")
        val input = Files.newInputStream(source)
        try inputStore.load(input, password)
        finally input.close()
        val aliases = inputStore.aliases()
        while aliases.hasMoreElements do
          val alias = aliases.nextElement()
          Option(inputStore.getCertificate(alias)).foreach { certificate =>
            combined.setCertificateEntry(s"source-$index-$alias", certificate)
          }
      }
      val path = directory.resolve(name)
      val output = Files.newOutputStream(path)
      try combined.store(output, password)
      finally output.close()
      path
    finally Arrays.fill(password, '\u0000')

  def deleteTree(root: Path): Unit =
    if Files.exists(root) then
      val deletion = Files.walk(root)
      try
        import scala.jdk.CollectionConverters.*
        deletion.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(path => Files.deleteIfExists(path): Unit)
      finally deletion.close()

  private def runKeytool(arguments: String*): Unit =
    val javaHome = Path.of(System.getProperty("java.home"))
    val executable = javaHome.resolve("bin").resolve(if System.getProperty("os.name").startsWith("Windows") then "keytool.exe" else "keytool")
    val process = ProcessBuilder((executable.toString +: arguments)*).redirectErrorStream(true).start()
    val output = String(process.getInputStream.readAllBytes())
    val exitCode = process.waitFor()
    if exitCode != 0 then throw IllegalStateException(s"keytool failed ($exitCode): $output")

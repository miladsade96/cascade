package cascade.security

import java.nio.file.{Files, Path}

object SecurityTestSupport:
  val StorePassword = "cascade-test-password"

  def createKeyStore(directory: Path, name: String = "broker.p12"): Path =
    val path = directory.resolve(name)
    val javaHome = Path.of(System.getProperty("java.home"))
    val executable = javaHome.resolve("bin").resolve(if System.getProperty("os.name").startsWith("Windows") then "keytool.exe" else "keytool")
    val process = new ProcessBuilder(
      executable.toString,
      "-genkeypair",
      "-alias", "cascade",
      "-keyalg", "RSA",
      "-keysize", "2048",
      "-validity", "2",
      "-dname", "CN=localhost, OU=Test, O=Cascade, L=Test, ST=Test, C=US",
      "-ext", "SAN=dns:localhost,ip:127.0.0.1",
      "-storetype", "PKCS12",
      "-keystore", path.toString,
      "-storepass", StorePassword,
      "-keypass", StorePassword,
      "-noprompt"
    ).redirectErrorStream(true).start()
    val output = String(process.getInputStream.readAllBytes())
    val exitCode = process.waitFor()
    if exitCode != 0 then throw IllegalStateException(s"keytool failed ($exitCode): $output")
    path

  def deleteTree(root: Path): Unit =
    if Files.exists(root) then
      val deletion = Files.walk(root)
      try
        import scala.jdk.CollectionConverters.*
        deletion.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(path => Files.deleteIfExists(path): Unit)
      finally deletion.close()

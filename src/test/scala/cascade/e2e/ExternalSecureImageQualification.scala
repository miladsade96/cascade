package cascade.e2e

import cascade.security.*
import java.net.ServerSocket
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.time.Duration
import java.util.{Properties, UUID}
import java.util.concurrent.{ExecutionException, TimeUnit}
import javax.net.ssl.SSLSocket
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.common.errors.SaslAuthenticationException
import scala.jdk.CollectionConverters.*

/** Kafka clients run on the host JDK; only the actual image runs the broker. */
object ExternalSecureImageQualification:
  def main(arguments: Array[String]): Unit =
    require(arguments.length == 1 && arguments(0).matches("sha256:[a-f0-9]{64}"), "expected immutable local image ID")
    val directory = Files.createTempDirectory("cascade-image-tls-")
    val keys = Files.createDirectory(directory.resolve("keys"))
    val name = s"cascade-secure-${UUID.randomUUID()}"
    val volume = s"$name-data"
    val socket = ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    var sequence = 0
    def docker(args: String*): String =
      sequence += 1
      val output = directory.resolve(s"command-$sequence.log")
      val process = ProcessBuilder((Vector("docker") ++ args)*).redirectErrorStream(true).redirectOutput(output.toFile).start()
      if !process.waitFor(150, TimeUnit.SECONDS) then
        process.destroyForcibly()
        throw IllegalStateException(s"Docker command timed out: ${args.head}")
      val result = Files.readString(output)
      require(process.exitValue() == 0, s"Docker ${args.head} failed: $result")
      result.trim
    def ready(): Unit =
      val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60)
      while System.nanoTime() < deadline && docker("inspect", "--format", "{{.State.Health.Status}}", name) != "healthy" do Thread.sleep(500)
      require(docker("inspect", "--format", "{{.State.Health.Status}}", name) == "healthy", "secure image failed readiness")
    try
      val store = SecurityTestSupport.createKeyStore(keys)
      Files.writeString(keys.resolve("password"), SecurityTestSupport.StorePassword)
      val password = "disposable-image-qualification-password".toCharArray
      try
        Files.writeString(keys.resolve("plain.conf"), CredentialTool.generateLine("alice", password) + "\n")
        Files.writeString(keys.resolve("scram.conf"), Vector(SaslMechanism.ScramSha256, SaslMechanism.ScramSha512)
          .map(mechanism => CredentialTool.generateScramLine("alice", password, mechanism)).mkString("", "\n", "\n"))
      finally java.util.Arrays.fill(password, '\u0000')
      if Files.getFileStore(keys).supportsFileAttributeView("posix") then
        Files.setPosixFilePermissions(keys, PosixFilePermissions.fromString("rwxr-xr-x"))
        val paths = Files.list(keys)
        try paths.iterator().asScala.foreach(path => Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rw-r--r--")): Unit)
        finally paths.close()
      docker("volume", "create", volume)
      docker("run", "--detach", "--name", name, "--read-only", "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
        "--memory", "2g", "--tmpfs", "/tmp:size=64m,mode=1777,nosuid,nodev,noexec",
        "--publish", s"127.0.0.1:$port:9092", "--mount", s"type=bind,source=$keys,target=/keys,readonly",
        "--mount", s"type=volume,source=$volume,target=/var/lib/cascade", arguments(0),
        "--host", "0.0.0.0", "--port", "9092", "--advertised-host", "127.0.0.1", "--advertised-port", port.toString,
        "--data-dir", "/var/lib/cascade", "--operations-port", "9404", "--flush-policy", "sync",
        "--security-protocol", "SASL_SSL", "--ssl-keystore", "/keys/broker.p12", "--ssl-keystore-password-file", "/keys/password",
        "--sasl-mechanisms", "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512", "--credentials-file", "/keys/plain.conf", "--scram-credentials-file", "/keys/scram.conf")
      ready()
      val tls = TlsContextFactory.create(TlsConfig(keyStore = Some(store), keyStorePassword = Some(SecurityTestSupport.StorePassword),
        trustStore = Some(store), trustStorePassword = Some(SecurityTestSupport.StorePassword)))
      for protocol <- Vector("TLSv1.2", "TLSv1.3") do
        val connection = tls.getSocketFactory.createSocket("127.0.0.1", port).asInstanceOf[SSLSocket]
        try
          connection.setSoTimeout(5000)
          connection.setEnabledProtocols(Array(protocol))
          val parameters = connection.getSSLParameters
          parameters.setEndpointIdentificationAlgorithm("HTTPS")
          connection.setSSLParameters(parameters)
          connection.startHandshake()
          require(connection.getSession.getProtocol == protocol, "unexpected TLS protocol")
        finally connection.close()
      def properties(mechanism: String, password: String): Properties =
        val result = Properties()
        result.setProperty("bootstrap.servers", s"127.0.0.1:$port")
        result.setProperty("security.protocol", "SASL_SSL")
        result.setProperty("ssl.truststore.location", store.toString)
        result.setProperty("ssl.truststore.type", "PKCS12")
        result.setProperty("ssl.truststore.password", SecurityTestSupport.StorePassword)
        result.setProperty("sasl.mechanism", mechanism)
        val loginModule = if mechanism == "PLAIN" then "org.apache.kafka.common.security.plain.PlainLoginModule" else "org.apache.kafka.common.security.scram.ScramLoginModule"
        result.setProperty("sasl.jaas.config", s"$loginModule required username=\"alice\" password=\"$password\";")
        result.setProperty("default.api.timeout.ms", "5000")
        result.setProperty("request.timeout.ms", "3000")
        result
      val mechanisms = Vector("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512")
      mechanisms.foreach { mechanism =>
        val valid = properties(mechanism, "disposable-image-qualification-password")
        ExternalBrokerSmokeTest.verify(s"127.0.0.1:$port", s"image-${mechanism.toLowerCase}", false, valid)
        val invalid = Admin.create(properties(mechanism, "wrong-password"))
        try
          var denied = false
          try invalid.describeCluster().nodes().get(10, TimeUnit.SECONDS)
          catch case error: ExecutionException if error.getCause.isInstanceOf[SaslAuthenticationException] => denied = true
          require(denied, s"$mechanism accepted invalid credentials")
        finally invalid.close(Duration.ofSeconds(2))
      }
      docker("restart", "--timeout", "120", name)
      ready()
      mechanisms.foreach { mechanism =>
        ExternalBrokerSmokeTest.verify(s"127.0.0.1:$port", s"image-${mechanism.toLowerCase}", true,
          properties(mechanism, "disposable-image-qualification-password"))
      }
      println(s"EXTERNAL_IMAGE_SECURITY_RESULT passed image=${arguments(0)} tls=1.2,1.3 mechanisms=3 records_each=25 denied_credentials=3 restart_records=75")
    finally
      val owned = docker("container", "ls", "--all", "--quiet", "--filter", s"name=^/$name$$")
      if owned.nonEmpty then { docker("rm", "--force", name): Unit }
      val ownedVolume = docker("volume", "ls", "--quiet", "--filter", s"name=^$volume$$")
      if ownedVolume.nonEmpty then { docker("volume", "rm", volume): Unit }
      SecurityTestSupport.deleteTree(directory)

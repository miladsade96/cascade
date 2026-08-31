package cascade.operations

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.Duration
import scala.util.control.NonFatal

final case class ContainerHealthCheckConfig(
    host: String,
    port: Int,
    timeoutMillis: Int,
    tokenFile: Option[Path]
)

object ContainerHealthCheck:
  private val DefaultHost = "127.0.0.1"
  private val DefaultPort = 9404
  private val DefaultTimeoutMillis = 2000

  def main(arguments: Array[String]): Unit =
    val result =
      for
        config <- parseEnvironment(sys.env)
        _ <- probe(config)
      yield ()
    result.left.foreach { message =>
      System.err.println(s"Cascade readiness probe failed: $message")
      System.exit(1)
    }

  private[operations] def parseEnvironment(environment: Map[String, String]): Either[String, ContainerHealthCheckConfig] =
    for
      host <- nonEmpty(environment.getOrElse("CASCADE_HEALTHCHECK_HOST", DefaultHost), "health-check host")
      port <- integer(environment.getOrElse("CASCADE_OPERATIONS_PORT", DefaultPort.toString), "operations port", 1, 65535)
      timeout <- integer(
        environment.getOrElse("CASCADE_HEALTHCHECK_TIMEOUT_MS", DefaultTimeoutMillis.toString),
        "health-check timeout",
        100,
        60000
      )
    yield ContainerHealthCheckConfig(
      host = host,
      port = port,
      timeoutMillis = timeout,
      tokenFile = environment.get("CASCADE_HEALTHCHECK_TOKEN_FILE").filter(_.nonEmpty).map(Paths.get(_))
    )

  private[operations] def probe(config: ContainerHealthCheckConfig): Either[String, Unit] =
    try
      val timeout = Duration.ofMillis(config.timeoutMillis.toLong)
      val client = HttpClient.newBuilder().connectTimeout(timeout).build()
      val requestBuilder = HttpRequest
        .newBuilder(URI("http", null, config.host, config.port, "/ready", null, null))
        .timeout(timeout)
        .GET()
      config.tokenFile.foreach { path =>
        val token = Files.readString(path, StandardCharsets.UTF_8).stripTrailing()
        if token.nonEmpty then requestBuilder.header("Authorization", s"Bearer $token"): Unit
      }
      val response = client.send(requestBuilder.build(), HttpResponse.BodyHandlers.discarding())
      if response.statusCode() == 200 then Right(())
      else Left(s"HTTP ${response.statusCode()}")
    catch
      case NonFatal(error) => Left(Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName))

  private def nonEmpty(value: String, description: String): Either[String, String] =
    val normalized = value.trim
    Either.cond(normalized.nonEmpty, normalized, s"$description must not be empty")

  private def integer(value: String, description: String, minimum: Int, maximum: Int): Either[String, Int] =
    value.toIntOption.filter(number => number >= minimum && number <= maximum).toRight(
      s"$description must be an integer from $minimum through $maximum"
    )

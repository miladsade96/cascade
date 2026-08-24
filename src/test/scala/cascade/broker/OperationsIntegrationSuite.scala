package cascade.broker

import cascade.operations.OperationsConfig
import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.nio.file.Files
import scala.jdk.CollectionConverters.*

final class OperationsIntegrationSuite extends munit.FunSuite:
  test("broker publishes live operational state and traffic metrics") {
    val directory = Files.createTempDirectory("cascade-operations-integration")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory,
        operations = OperationsConfig(port = Some(0), logToStderr = false)
      )
    )
    try
      broker.start()
      val port = broker.operationsPort.getOrElse(fail("operations listener did not start"))

      val live = get(port, "/live")
      assertEquals(live.statusCode(), 200)
      assert(live.body().contains("\"broker_running\""))

      val ready = eventuallyReady(port)
      assertEquals(ready.statusCode(), 200)
      assert(ready.body().contains("\"status\":\"ok\""))

      val status = get(port, "/v1/status")
      assertEquals(status.statusCode(), 200)
      assert(status.body().contains(s"\"node_id\":${broker.config.nodeId}"))

      val metrics = get(port, "/metrics")
      assertEquals(metrics.statusCode(), 200)
      assert(metrics.body().contains("cascade_disk_usable_bytes"))
      assert(metrics.body().contains("cascade_broker_up"))
    finally
      broker.close()
      deleteTree(directory)
  }

  private def eventuallyReady(port: Int): HttpResponse[String] =
    val deadline = System.nanoTime() + 5_000_000_000L
    var response = get(port, "/ready")
    while response.statusCode() != 200 && System.nanoTime() < deadline do
      Thread.sleep(25L)
      response = get(port, "/ready")
    response

  private def get(port: Int, path: String): HttpResponse[String] =
    val request = HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:$port$path")).GET().build()
    HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

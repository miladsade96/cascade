package cascade.operations

import java.net.URI
import java.net.{InetAddress, ServerSocket}
import java.net.http.{HttpClient, HttpRequest, HttpResponse}

class OperationsServerSuite extends munit.FunSuite:
  private val token = "a-secure-operations-token-with-32-characters"

  test("serves authenticated health, status, and Prometheus responses") {
    val server = OperationsServer(
      OperationsConfig(port = Some(0), authenticationToken = Some(token), logToStderr = false),
      () => snapshot(),
      () => BrokerHealth.evaluate(snapshot(), HealthPolicy(1024L, 0L), None)
    )
    server.start()
    try
      val unauthorized = request(server, "/metrics")
      assertEquals(unauthorized.statusCode(), 401)
      assertEquals(unauthorized.headers().firstValue("WWW-Authenticate").orElse(""), "Bearer")

      val live = request(server, "/live", Some(token))
      assertEquals(live.statusCode(), 200)
      assert(live.body().contains("\"status\":\"ok\""))

      val metrics = request(server, "/metrics", Some(token))
      assertEquals(metrics.statusCode(), 200)
      assert(metrics.headers().firstValue("Content-Type").orElse("").startsWith("text/plain"))
      assert(metrics.body().contains("cascade_broker_up{node_id=\"7\"} 1.0"))

      val status = request(server, "/v1/status", Some(token))
      assertEquals(status.statusCode(), 200)
      assert(status.body().contains("\"node_id\":7"))
      assert(status.body().contains("\"peer_authentications\":0"))
      assert(status.body().contains("\"peer_authentication_rejections\":0"))
      assertEquals(status.headers().firstValue("Cache-Control").orElse(""), "no-store")
    finally server.close()
  }

  test("returns a service-unavailable readiness response with failed checks") {
    val current = snapshot(brokerFenced = true)
    val server = OperationsServer(
      OperationsConfig(port = Some(0), logToStderr = false),
      () => current,
      () => BrokerHealth.evaluate(current, HealthPolicy(1024L, 0L), None)
    )
    server.start()
    try
      val response = request(server, "/ready")
      assertEquals(response.statusCode(), 503)
      assert(response.body().contains("broker_unfenced"))
      assert(response.body().contains("\"status\":\"failed\""))
    finally server.close()
  }

  test("rejects unsupported methods and paths") {
    val server = OperationsServer(
      OperationsConfig(port = Some(0), logToStderr = false),
      () => snapshot(),
      () => BrokerHealth.evaluate(snapshot(), HealthPolicy(1024L, 0L), None)
    )
    server.start()
    try
      val client = HttpClient.newHttpClient()
      val post = HttpRequest.newBuilder(uri(server, "/live")).POST(HttpRequest.BodyPublishers.noBody()).build()
      assertEquals(client.send(post, HttpResponse.BodyHandlers.ofString()).statusCode(), 405)
      assertEquals(request(server, "/live/extra").statusCode(), 404)
    finally server.close()
  }

  test("does not bind before start and close leaves the configured port available") {
    val reservation = ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))
    val port = reservation.getLocalPort
    reservation.close()
    val server = OperationsServer(
      OperationsConfig(port = Some(port), logToStderr = false),
      () => snapshot(),
      () => BrokerHealth.evaluate(snapshot(), HealthPolicy(1024L, 0L), None)
    )
    server.close()

    val replacement = ServerSocket(port, 1, InetAddress.getByName("127.0.0.1"))
    try assertEquals(replacement.getLocalPort, port)
    finally replacement.close()
  }

  private def request(server: OperationsServer, path: String, bearer: Option[String] = None): HttpResponse[String] =
    val builder = HttpRequest.newBuilder(uri(server, path)).GET()
    bearer.foreach(value => builder.header("Authorization", s"Bearer $value"))
    HttpClient.newHttpClient().send(builder.build(), HttpResponse.BodyHandlers.ofString())

  private def uri(server: OperationsServer, path: String): URI = URI.create(s"http://127.0.0.1:${server.boundPort}$path")

  private def snapshot(brokerFenced: Boolean = false): BrokerMetricsSnapshot =
    BrokerMetricsSnapshot(
      7, 100L, true, false, 7, brokerFenced, 2, 4, 1, 0L, 0, 0L, 0, 0L, 0L, 0L,
      TrafficSnapshot(1L, 2L, 1L, 3L, 0L, 4L), 1L, 2L, 3L, 0L, 1L, 0L, 0L, 0L,
      1024L, 2048L, 128L, 4096L
    )

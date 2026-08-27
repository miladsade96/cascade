package cascade.operations

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.concurrent.{ExecutorService, Executors}
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

final class OperationsServer(
    config: OperationsConfig,
    snapshot: () => BrokerMetricsSnapshot,
    health: () => BrokerHealth,
    onError: Throwable => Unit = _ => ()
) extends AutoCloseable:
  config.validate(): Unit
  private val configuredPort = config.port.getOrElse(throw IllegalArgumentException("operations port is not configured"))
  private val executor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
  private val server = HttpServer.create()
  private val started = AtomicBoolean(false)
  private val closed = AtomicBoolean(false)
  server.setExecutor(executor)
  register("/live") { current =>
    val state = health()
    HttpResult(if state.live then 200 else 503, OperationsJson.health("liveness", state.live, state))
  }
  register("/ready") { current =>
    val state = health()
    HttpResult(if state.ready then 200 else 503, OperationsJson.health("readiness", state.ready, state))
  }
  register("/metrics") { _ => HttpResult(200, PrometheusMetrics.encode(snapshot()), PrometheusMetrics.ContentType) }
  register("/v1/status") { _ =>
    val current = snapshot()
    HttpResult(200, OperationsJson.status(current, health()))
  }

  def start(): Unit =
    if closed.get() then throw IllegalStateException("operations server is closed")
    if !started.compareAndSet(false, true) then throw IllegalStateException("operations server is already running")
    try
      server.bind(InetSocketAddress(config.bindHost, configuredPort), 64)
      server.start()
    catch
      case error: Throwable =>
        server.stop(0)
        executor.shutdownNow(): Unit
        closed.set(true)
        throw error

  def boundPort: Int = server.getAddress.getPort

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      if started.get() then server.stop(1)
      executor.shutdownNow(): Unit

  private def register(path: String)(result: HttpExchange => HttpResult): Unit =
    server.createContext(
      path,
      new HttpHandler:
        override def handle(exchange: HttpExchange): Unit =
          try
            if exchange.getRequestURI.getPath != path then send(exchange, HttpResult(404, OperationsJson.error("not_found")))
            else if exchange.getRequestMethod != "GET" then
              exchange.getResponseHeaders.set("Allow", "GET")
              send(exchange, HttpResult(405, OperationsJson.error("method_not_allowed")))
            else if !authorized(exchange) then
              exchange.getResponseHeaders.set("WWW-Authenticate", "Bearer")
              send(exchange, HttpResult(401, OperationsJson.error("unauthorized")))
            else send(exchange, result(exchange))
          catch
            case NonFatal(error) =>
              onError(error)
              try send(exchange, HttpResult(500, OperationsJson.error("internal_error")))
              catch case NonFatal(_) => exchange.close()
    ): Unit

  private def authorized(exchange: HttpExchange): Boolean =
    config.authenticationToken match
      case None => true
      case Some(expected) =>
        val supplied = Option(exchange.getRequestHeaders.getFirst("Authorization")).collect {
          case value if value.startsWith("Bearer ") => value.substring("Bearer ".length)
        }.getOrElse("")
        MessageDigest.isEqual(
          supplied.getBytes(StandardCharsets.UTF_8),
          expected.getBytes(StandardCharsets.UTF_8)
        )

  private def send(exchange: HttpExchange, result: HttpResult): Unit =
    val bytes = result.body.getBytes(StandardCharsets.UTF_8)
    val headers = exchange.getResponseHeaders
    headers.set("Content-Type", result.contentType)
    headers.set("Cache-Control", "no-store")
    headers.set("X-Content-Type-Options", "nosniff")
    exchange.sendResponseHeaders(result.status, bytes.length.toLong)
    val body = exchange.getResponseBody
    try body.write(bytes)
    finally
      body.close()
      exchange.close()

private final case class HttpResult(
    status: Int,
    body: String,
    contentType: String = "application/json; charset=utf-8"
)

private object OperationsJson:
  def health(kind: String, healthy: Boolean, state: BrokerHealth): String =
    s"{\"check\":\"${escape(kind)}\",\"status\":\"${if healthy then "ok" else "failed"}\",\"checks\":${checks(state)}}"

  def status(snapshot: BrokerMetricsSnapshot, health: BrokerHealth): String =
    s"""{"node_id":${snapshot.nodeId},"uptime_ms":${snapshot.uptimeMillis},"running":${snapshot.running},"ready":${health.ready},"clustered":${snapshot.clustered},"controller_id":${snapshot.controllerId},"broker_fenced":${snapshot.brokerFenced},"topics":${snapshot.topics},"partitions":${snapshot.partitions},"active_connections":${snapshot.activeConnections},"inflight_requests":${snapshot.activeRequests},"sasl_authentication_successes":${snapshot.authentication.successes},"sasl_authentication_failures":${snapshot.authentication.failures},"peer_authentications":${snapshot.peerSecurity.authenticated},"peer_tls_authentications":${snapshot.peerSecurity.tlsAuthenticated},"peer_authentication_rejections":${snapshot.peerSecurity.rejected},"pending_flush_bytes":${snapshot.pendingFlushBytes},"usable_disk_bytes":${snapshot.usableDiskBytes},"checks":${checks(health)}}"""

  def error(code: String): String = s"{\"error\":\"${escape(code)}\"}"

  private def checks(state: BrokerHealth): String =
    state.checks.map { check =>
      s"{\"name\":\"${escape(check.name)}\",\"healthy\":${check.healthy},\"detail\":\"${escape(check.detail)}\"}"
    }.mkString("[", ",", "]")

  private def escape(value: String): String =
    val result = StringBuilder(value.length + 16)
    value.foreach {
      case '"'  => result.append("\\\"")
      case '\\' => result.append("\\\\")
      case '\n' => result.append("\\n")
      case '\r' => result.append("\\r")
      case '\t' => result.append("\\t")
      case character if character < ' ' => result.append(f"\\u${character.toInt}%04x")
      case character => result.append(character)
    }
    result.result()

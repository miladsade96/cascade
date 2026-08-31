package cascade.operations

import cascade.security.{RequestQuotaSnapshot, TlsReloadSnapshot}
import java.util.concurrent.atomic.AtomicLong

final case class TrafficSnapshot(
    requests: Long,
    requestBytes: Long,
    responses: Long,
    responseBytes: Long,
    failures: Long,
    requestNanos: Long
)

final class TrafficMetrics:
  private val requests = AtomicLong(0L)
  private val requestBytes = AtomicLong(0L)
  private val responses = AtomicLong(0L)
  private val responseBytes = AtomicLong(0L)
  private val failures = AtomicLong(0L)
  private val requestNanos = AtomicLong(0L)

  def recordRequest(bytes: Int): Unit =
    requests.incrementAndGet(): Unit
    requestBytes.addAndGet(bytes.toLong): Unit

  def recordResponse(bytes: Int): Unit =
    responses.incrementAndGet(): Unit
    responseBytes.addAndGet(bytes.toLong): Unit

  def recordFailure(): Unit = failures.incrementAndGet(): Unit

  def recordDuration(nanos: Long): Unit = requestNanos.addAndGet(math.max(0L, nanos)): Unit

  def snapshot: TrafficSnapshot =
    TrafficSnapshot(
      requests.get(),
      requestBytes.get(),
      responses.get(),
      responseBytes.get(),
      failures.get(),
      requestNanos.get()
    )

final case class TrafficQuotaSnapshot(
    request: RequestQuotaSnapshot,
    response: RequestQuotaSnapshot,
    produce: RequestQuotaSnapshot,
    fetch: RequestQuotaSnapshot
)

object TrafficQuotaSnapshot:
  val Empty: TrafficQuotaSnapshot = TrafficQuotaSnapshot(
    RequestQuotaSnapshot.Empty,
    RequestQuotaSnapshot.Empty,
    RequestQuotaSnapshot.Empty,
    RequestQuotaSnapshot.Empty
  )

final case class BrokerMetricsSnapshot(
    nodeId: Int,
    uptimeMillis: Long,
    running: Boolean,
    clustered: Boolean,
    controllerId: Int,
    brokerFenced: Boolean,
    topics: Int,
    partitions: Int,
    activeConnections: Int,
    rejectedConnections: Long,
    activeRequests: Int,
    rejectedRequests: Long,
    quotaPrincipals: Int,
    quotaThrottledRequests: Long,
    quotaRejectedRequests: Long,
    quotaThrottleMillis: Long,
    traffic: TrafficSnapshot,
    flushOperations: Long,
    flushBytes: Long,
    flushNanos: Long,
    pendingFlushBytes: Long,
    lifecycleRuns: Long,
    retiredSegments: Long,
    reclaimedBytes: Long,
    rejectedAppends: Long,
    usableDiskBytes: Long,
    totalDiskBytes: Long,
    heapUsedBytes: Long,
    heapMaxBytes: Long,
    peerSecurity: PeerSecuritySnapshot = PeerSecuritySnapshot.Empty,
    authentication: AuthenticationSnapshot = AuthenticationSnapshot.Empty,
    tlsReload: TlsReloadSnapshot = TlsReloadSnapshot.Empty,
    trafficQuotas: TrafficQuotaSnapshot = TrafficQuotaSnapshot.Empty
)

object PrometheusMetrics:
  val ContentType = "text/plain; version=0.0.4; charset=utf-8"

  def encode(snapshot: BrokerMetricsSnapshot): String =
    val labels = Map("node_id" -> snapshot.nodeId.toString)
    val builder = StringBuilder(4096)
    gauge(builder, "cascade_broker_up", "Whether the broker is running.", if snapshot.running then 1d else 0d, labels)
    gauge(builder, "cascade_broker_uptime_seconds", "Broker process uptime in seconds.", snapshot.uptimeMillis / 1000d, labels)
    gauge(builder, "cascade_broker_clustered", "Whether cluster mode is enabled.", if snapshot.clustered then 1d else 0d, labels)
    gauge(builder, "cascade_broker_controller_id", "Current metadata controller node ID.", snapshot.controllerId.toDouble, labels)
    gauge(builder, "cascade_broker_fenced", "Whether this broker is fenced.", if snapshot.brokerFenced then 1d else 0d, labels)
    gauge(builder, "cascade_topics", "Topics visible to this broker.", snapshot.topics.toDouble, labels)
    gauge(builder, "cascade_partitions", "Local partitions opened by this broker.", snapshot.partitions.toDouble, labels)
    gauge(builder, "cascade_connections_active", "Currently active client connections.", snapshot.activeConnections.toDouble, labels)
    counter(builder, "cascade_connections_rejected_total", "Connections rejected by admission limits.", snapshot.rejectedConnections.toDouble, labels)
    gauge(builder, "cascade_requests_inflight", "Currently executing Kafka requests.", snapshot.activeRequests.toDouble, labels)
    counter(builder, "cascade_requests_rejected_total", "Requests rejected by in-flight admission.", snapshot.rejectedRequests.toDouble, labels)
    counter(builder, "cascade_requests_total", "Kafka requests handled by this broker.", snapshot.traffic.requests.toDouble, labels)
    counter(builder, "cascade_request_bytes_total", "Kafka request bytes received including frame prefixes.", snapshot.traffic.requestBytes.toDouble, labels)
    counter(builder, "cascade_responses_total", "Kafka responses emitted by this broker.", snapshot.traffic.responses.toDouble, labels)
    counter(builder, "cascade_response_bytes_total", "Kafka response bytes emitted including frame prefixes.", snapshot.traffic.responseBytes.toDouble, labels)
    counter(builder, "cascade_request_failures_total", "Kafka request handler failures.", snapshot.traffic.failures.toDouble, labels)
    counter(builder, "cascade_request_processing_seconds_total", "Cumulative Kafka request processing time.", snapshot.traffic.requestNanos / 1_000_000_000d, labels)
    counter(builder, "cascade_peer_authentications_total", "Internal peer requests accepted by identity policy.", snapshot.peerSecurity.authenticated.toDouble, labels)
    counter(builder, "cascade_peer_tls_authentications_total", "Internal peer requests accepted over mutually authenticated TLS.", snapshot.peerSecurity.tlsAuthenticated.toDouble, labels)
    counter(builder, "cascade_peer_authentication_rejections_total", "Internal peer requests rejected by peer authentication.", snapshot.peerSecurity.rejected.toDouble, labels)
    gauge(builder, "cascade_tls_enabled", "Whether the Kafka listener uses TLS.", if snapshot.tlsReload.enabled then 1d else 0d, labels)
    gauge(builder, "cascade_tls_material_generation", "Active TLS key and trust material generation.", snapshot.tlsReload.generation.toDouble, labels)
    counter(builder, "cascade_tls_material_reloads_total", "Successful atomic TLS material reloads.", snapshot.tlsReload.successfulReloads.toDouble, labels)
    counter(builder, "cascade_tls_material_reload_failures_total", "Rejected TLS material reloads.", snapshot.tlsReload.failedReloads.toDouble, labels)
    snapshot.authentication.mechanisms.foreach { mechanism =>
      val mechanismLabels = labels.updated("mechanism", mechanism.mechanism)
      counter(builder, "cascade_sasl_authentication_successes_total", "Successful SASL authentications.", mechanism.successes.toDouble, mechanismLabels)
      counter(builder, "cascade_sasl_authentication_failures_total", "Failed SASL authentications.", mechanism.failures.toDouble, mechanismLabels)
    }
    gauge(builder, "cascade_quota_principals", "Principals with active request quota buckets.", snapshot.quotaPrincipals.toDouble, labels)
    counter(builder, "cascade_quota_throttled_requests_total", "Requests delayed by principal quotas.", snapshot.quotaThrottledRequests.toDouble, labels)
    counter(builder, "cascade_quota_rejected_requests_total", "Requests shed because required quota delay was too large.", snapshot.quotaRejectedRequests.toDouble, labels)
    counter(builder, "cascade_quota_throttle_seconds_total", "Cumulative principal quota delay.", snapshot.quotaThrottleMillis / 1000d, labels)
    Vector(
      "request" -> snapshot.trafficQuotas.request,
      "response" -> snapshot.trafficQuotas.response,
      "produce" -> snapshot.trafficQuotas.produce,
      "fetch" -> snapshot.trafficQuotas.fetch
    ).foreach { case (quota, value) =>
      val quotaLabels = labels.updated("quota", quota)
      gauge(builder, "cascade_traffic_quota_principals", "Principals with active traffic quota buckets.", value.principals.toDouble, quotaLabels)
      counter(builder, "cascade_traffic_quota_throttled_total", "Traffic quota reservations delayed.", value.throttled.toDouble, quotaLabels)
      counter(builder, "cascade_traffic_quota_rejected_total", "Traffic quota reservations rejected.", value.rejected.toDouble, quotaLabels)
      counter(builder, "cascade_traffic_quota_throttle_seconds_total", "Cumulative traffic quota delay.", value.throttleMillis / 1000d, quotaLabels)
    }
    counter(builder, "cascade_flush_operations_total", "Completed storage force operations.", snapshot.flushOperations.toDouble, labels)
    counter(builder, "cascade_flush_bytes_total", "Bytes covered by completed force operations.", snapshot.flushBytes.toDouble, labels)
    counter(builder, "cascade_flush_seconds_total", "Cumulative storage force duration.", snapshot.flushNanos / 1_000_000_000d, labels)
    gauge(builder, "cascade_flush_pending_bytes", "Bytes awaiting a storage force.", snapshot.pendingFlushBytes.toDouble, labels)
    counter(builder, "cascade_lifecycle_runs_total", "Storage lifecycle passes.", snapshot.lifecycleRuns.toDouble, labels)
    counter(builder, "cascade_lifecycle_retired_segments_total", "Segments retired by storage lifecycle.", snapshot.retiredSegments.toDouble, labels)
    counter(builder, "cascade_lifecycle_reclaimed_bytes_total", "Bytes reclaimed by storage lifecycle.", snapshot.reclaimedBytes.toDouble, labels)
    counter(builder, "cascade_storage_rejected_appends_total", "Appends rejected by disk-reserve admission.", snapshot.rejectedAppends.toDouble, labels)
    gauge(builder, "cascade_disk_usable_bytes", "Usable bytes on the broker data volume.", snapshot.usableDiskBytes.toDouble, labels)
    gauge(builder, "cascade_disk_total_bytes", "Total bytes on the broker data volume.", snapshot.totalDiskBytes.toDouble, labels)
    gauge(builder, "cascade_jvm_heap_used_bytes", "JVM heap bytes currently used.", snapshot.heapUsedBytes.toDouble, labels)
    gauge(builder, "cascade_jvm_heap_max_bytes", "Maximum JVM heap bytes.", snapshot.heapMaxBytes.toDouble, labels)
    builder.result()

  private def gauge(
      builder: StringBuilder,
      name: String,
      help: String,
      value: Double,
      labels: Map[String, String]
  ): Unit = metric(builder, name, help, "gauge", value, labels)

  private def counter(
      builder: StringBuilder,
      name: String,
      help: String,
      value: Double,
      labels: Map[String, String]
  ): Unit = metric(builder, name, help, "counter", value, labels)

  private def metric(
      builder: StringBuilder,
      name: String,
      help: String,
      metricType: String,
      value: Double,
      labels: Map[String, String]
  ): Unit =
    builder.append("# HELP ").append(name).append(' ').append(escapeHelp(help)).append('\n')
    builder.append("# TYPE ").append(name).append(' ').append(metricType).append('\n')
    builder.append(name).append(encodeLabels(labels)).append(' ').append(format(value)).append('\n'): Unit

  private def encodeLabels(labels: Map[String, String]): String =
    labels.toVector.sortBy(_._1).map { case (key, value) => s"$key=\"${escapeLabel(value)}\"" }.mkString("{", ",", "}")

  private def escapeHelp(value: String): String = value.replace("\\", "\\\\").replace("\n", "\\n")

  private def escapeLabel(value: String): String = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"")

  private def format(value: Double): String =
    if value.isPosInfinity then "+Inf"
    else if value.isNegInfinity then "-Inf"
    else if value.isNaN then "NaN"
    else java.lang.Double.toString(value)

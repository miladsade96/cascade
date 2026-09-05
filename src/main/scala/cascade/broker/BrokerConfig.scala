package cascade.broker

import cascade.cluster.ClusterNode
import cascade.coordinator.CoordinatorPublicationConfig
import cascade.group.OffsetBatchConfig
import cascade.operations.OperationsConfig
import cascade.security.*
import cascade.storage.{CleanupPolicy, FlushPolicy, StorageLifecycleConfig}
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

final case class BrokerConfig(
    bindHost: String = "0.0.0.0",
    port: Int = 9092,
    advertisedHost: String = "localhost",
    advertisedPort: Option[Int] = None,
    dataDirectory: Path = Paths.get("data"),
    maxRequestBytes: Int = 100 * 1024 * 1024,
    segmentBytes: Long = 128L * 1024 * 1024,
    flushPolicy: FlushPolicy = FlushPolicy.Periodic,
    flushIntervalMillis: Long = 1000L,
    flushBytes: Long = 64L * 1024 * 1024,
    nodeId: Int = 1,
    clusterNodes: Vector[ClusterNode] = Vector.empty,
    controllerId: Int = 1,
    defaultReplicationFactor: Int = 1,
    minInSyncReplicas: Int = 1,
    peerTimeoutMillis: Int = 3000,
    replicaRecoveryTimeoutMillis: Int = 300000,
    replicaRecoveryChunkBytes: Int = 8 * 1024 * 1024,
    controllerHeartbeatMillis: Int = 250,
    controllerElectionTimeoutMillis: Int = 1500,
    storageLifecycle: StorageLifecycleConfig = StorageLifecycleConfig(),
    security: BrokerSecurityConfig = BrokerSecurityConfig(),
    operations: OperationsConfig = OperationsConfig(),
    autoCreateTopics: Boolean = true,
    offsetBatch: OffsetBatchConfig = OffsetBatchConfig(),
    coordinatorPublication: CoordinatorPublicationConfig = CoordinatorPublicationConfig()
):
  require(port >= 0 && port <= 65535, "port must be between 0 and 65535")
  require(advertisedPort.forall(value => value > 0 && value <= 65535), "advertised port must be valid")
  require(maxRequestBytes >= 1024, "max request size must be at least 1 KiB")
  require(flushIntervalMillis > 0, "flush interval must be positive")
  require(flushBytes > 0, "flush bytes must be positive")
  require(defaultReplicationFactor > 0, "default replication factor must be positive")
  require(minInSyncReplicas > 0, "minimum in-sync replicas must be positive")
  require(peerTimeoutMillis > 0, "peer timeout must be positive")
  require(replicaRecoveryTimeoutMillis > 0, "replica recovery timeout must be positive")
  require(
    replicaRecoveryChunkBytes > 0 && replicaRecoveryChunkBytes <= maxRequestBytes,
    "replica recovery chunk size must be positive and no larger than the request limit"
  )
  require(controllerHeartbeatMillis > 0, "controller heartbeat interval must be positive")
  require(
    controllerElectionTimeoutMillis.toLong >= controllerHeartbeatMillis.toLong * 3L,
    "controller election timeout must be at least three heartbeat intervals"
  )
  require(clusterNodes.map(_.id).distinct.size == clusterNodes.size, "cluster node IDs must be unique")
  require(clusterNodes.isEmpty || clusterNodes.exists(_.id == controllerId), "cluster nodes must contain the controller ID")
  require(
    clusterNodes.isEmpty || defaultReplicationFactor <= clusterNodes.size,
    "default replication factor cannot exceed cluster size"
  )
  require(
    minInSyncReplicas <= defaultReplicationFactor,
    "minimum in-sync replicas cannot exceed default replication factor"
  )

object BrokerConfig:
  def parse(arguments: Array[String]): BrokerConfig =
    @annotation.tailrec
    def loop(remaining: List[String], config: BrokerConfig): BrokerConfig = remaining match
      case Nil => config
      case "--host" :: value :: tail => loop(tail, config.copy(bindHost = value))
      case "--port" :: value :: tail => loop(tail, config.copy(port = value.toInt))
      case "--advertised-host" :: value :: tail => loop(tail, config.copy(advertisedHost = value))
      case "--advertised-port" :: value :: tail => loop(tail, config.copy(advertisedPort = Some(value.toInt)))
      case "--data-dir" :: value :: tail => loop(tail, config.copy(dataDirectory = Paths.get(value)))
      case "--max-request-bytes" :: value :: tail => loop(tail, config.copy(maxRequestBytes = value.toInt))
      case "--segment-bytes" :: value :: tail => loop(tail, config.copy(segmentBytes = value.toLong))
      case "--flush-policy" :: value :: tail => loop(tail, config.copy(flushPolicy = FlushPolicy.parse(value)))
      case "--flush-interval-ms" :: value :: tail => loop(tail, config.copy(flushIntervalMillis = value.toLong))
      case "--flush-bytes" :: value :: tail => loop(tail, config.copy(flushBytes = value.toLong))
      case "--node-id" :: value :: tail => loop(tail, config.copy(nodeId = value.toInt))
      case "--cluster-nodes" :: value :: tail =>
        loop(tail, config.copy(clusterNodes = value.split(',').toVector.map(ClusterNode.parse)))
      case "--controller-id" :: value :: tail => loop(tail, config.copy(controllerId = value.toInt))
      case "--default-replication-factor" :: value :: tail =>
        loop(tail, config.copy(defaultReplicationFactor = value.toInt))
      case "--min-insync-replicas" :: value :: tail => loop(tail, config.copy(minInSyncReplicas = value.toInt))
      case "--peer-timeout-ms" :: value :: tail => loop(tail, config.copy(peerTimeoutMillis = value.toInt))
      case "--replica-recovery-timeout-ms" :: value :: tail =>
        loop(tail, config.copy(replicaRecoveryTimeoutMillis = value.toInt))
      case "--replica-recovery-chunk-bytes" :: value :: tail =>
        loop(tail, config.copy(replicaRecoveryChunkBytes = value.toInt))
      case "--controller-heartbeat-ms" :: value :: tail =>
        loop(tail, config.copy(controllerHeartbeatMillis = value.toInt))
      case "--controller-election-timeout-ms" :: value :: tail =>
        loop(tail, config.copy(controllerElectionTimeoutMillis = value.toInt))
      case "--offset-batch-max-requests" :: value :: tail =>
        loop(tail, config.copy(offsetBatch = config.offsetBatch.copy(maxRequests = value.toInt)))
      case "--offset-batch-max-bytes" :: value :: tail =>
        loop(tail, config.copy(offsetBatch = config.offsetBatch.copy(maxBytes = value.toLong)))
      case "--offset-batch-pending-requests" :: value :: tail =>
        loop(tail, config.copy(offsetBatch = config.offsetBatch.copy(maxPendingRequests = value.toInt)))
      case "--offset-batch-pending-bytes" :: value :: tail =>
        loop(tail, config.copy(offsetBatch = config.offsetBatch.copy(maxPendingBytes = value.toLong)))
      case "--offset-batch-linger-ms" :: value :: tail =>
        loop(tail, config.copy(offsetBatch = config.offsetBatch.copy(lingerMillis = value.toLong)))
      case "--offset-batch-queue-timeout-ms" :: value :: tail =>
        loop(tail, config.copy(offsetBatch = config.offsetBatch.copy(queueTimeoutMillis = value.toLong)))
      case "--coordinator-publication-max-requests" :: value :: tail =>
        loop(tail, config.copy(coordinatorPublication = config.coordinatorPublication.copy(maxRequests = value.toInt)))
      case "--coordinator-publication-max-bytes" :: value :: tail =>
        loop(tail, config.copy(coordinatorPublication = config.coordinatorPublication.copy(maxBytes = value.toLong)))
      case "--coordinator-publication-pending-requests" :: value :: tail =>
        loop(tail, config.copy(coordinatorPublication = config.coordinatorPublication.copy(maxPendingRequests = value.toInt)))
      case "--coordinator-publication-pending-bytes" :: value :: tail =>
        loop(tail, config.copy(coordinatorPublication = config.coordinatorPublication.copy(maxPendingBytes = value.toLong)))
      case "--coordinator-publication-linger-ms" :: value :: tail =>
        loop(tail, config.copy(coordinatorPublication = config.coordinatorPublication.copy(lingerMillis = value.toLong)))
      case "--coordinator-publication-queue-timeout-ms" :: value :: tail =>
        loop(tail, config.copy(coordinatorPublication = config.coordinatorPublication.copy(queueTimeoutMillis = value.toLong)))
      case "--cleanup-policy" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(cleanupPolicy = CleanupPolicy.parse(value))))
      case "--retention-ms" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(retentionMillis = value.toLong)))
      case "--retention-bytes" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(retentionBytes = value.toLong)))
      case "--lifecycle-interval-ms" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(lifecycleIntervalMillis = value.toLong)))
      case "--minimum-free-bytes" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(minimumFreeBytes = value.toLong)))
      case "--offset-retention-ms" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(offsetRetentionMillis = value.toLong)))
      case "--journal-compaction-bytes" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(journalCompactionBytes = value.toLong)))
      case "--delete-retention-ms" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(deleteRetentionMillis = value.toLong)))
      case "--compaction-max-bytes-per-second" :: value :: tail =>
        loop(tail, config.copy(storageLifecycle = config.storageLifecycle.copy(compactionMaxBytesPerSecond = value.toLong)))
      case "--security-protocol" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(protocol = SecurityProtocol.parse(value))))
      case "--ssl-keystore" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(keyStore = Some(Paths.get(value))))))
      case "--ssl-keystore-password-file" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(keyStorePassword = Some(readSecret(value))))))
      case "--ssl-key-password-file" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(keyPassword = Some(readSecret(value))))))
      case "--ssl-truststore" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(trustStore = Some(Paths.get(value))))))
      case "--ssl-truststore-password-file" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(trustStorePassword = Some(readSecret(value))))))
      case "--ssl-client-auth" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(clientAuth = TlsClientAuth.parse(value)))))
      case "--tls-protocols" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(enabledProtocols = splitCsv(value)))))
      case "--ssl-reload-ms" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(tls = config.security.tls.copy(reloadIntervalMillis = value.toLong))))
      case "--peer-security-protocol" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(peer = config.security.peer.copy(protocol = PeerSecurityProtocol.parse(value)))))
      case "--peer-identity-file" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(peer = config.security.peer.copy(identityFile = Some(Paths.get(value))))))
      case "--peer-identity-reload-ms" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(peer = config.security.peer.copy(identityReloadIntervalMillis = value.toLong))))
      case "--credentials-file" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authentication = config.security.authentication.copy(credentialsFile = Some(Paths.get(value))))))
      case "--scram-credentials-file" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authentication = config.security.authentication.copy(scramCredentialsFile = Some(Paths.get(value))))))
      case "--sasl-mechanisms" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authentication = config.security.authentication.copy(mechanisms = splitCsv(value).map(SaslMechanism.parse)))))
      case "--oauth-jwks-uri" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(jwksUri = Some(URI.create(value)))))
      case "--oauth-issuer" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(issuer = Some(value))))
      case "--oauth-audience" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(audience = Some(value))))
      case "--oauth-principal-claim" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(principalClaim = value)))
      case "--oauth-scope-claim" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(scopeClaim = value)))
      case "--oauth-role-claim" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(roleClaim = Some(value))))
      case "--oauth-role-map" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(roleMappings = parseMappings(value))))
      case "--oauth-required-scopes" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(requiredScopes = splitCsv(value).toSet)))
      case "--oauth-allowed-algorithms" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(allowedAlgorithms = splitCsv(value).map(JwtAlgorithm.parse).toSet)))
      case "--oauth-clock-skew-seconds" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(clockSkewSeconds = value.toLong)))
      case "--oauth-jwks-refresh-ms" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(jwksRefreshMillis = value.toLong)))
      case "--oauth-http-timeout-ms" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(httpTimeoutMillis = value.toInt)))
      case "--oauth-max-token-bytes" :: value :: tail =>
        loop(tail, updateOAuth(config)(_.copy(maximumTokenBytes = value.toInt)))
      case "--credential-reload-ms" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authentication = config.security.authentication.copy(reloadIntervalMillis = value.toLong))))
      case "--sasl-session-lifetime-ms" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authentication = config.security.authentication.copy(sessionLifetimeMillis = value.toLong))))
      case "--acl-file" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authorization = config.security.authorization.copy(aclFile = Some(Paths.get(value))))))
      case "--acl-reload-ms" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authorization = config.security.authorization.copy(reloadIntervalMillis = value.toLong))))
      case "--super-users" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(authorization = config.security.authorization.copy(superUsers = splitCsv(value).toSet))))
      case "--audit-log" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(audit = config.security.audit.copy(path = Some(Paths.get(value))))))
      case "--audit-buffered" :: tail =>
        loop(tail, config.copy(security = config.security.copy(audit = config.security.audit.copy(forceEachEvent = false))))
      case "--max-connections" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(maxConnections = value.toInt))))
      case "--max-connections-per-ip" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(maxConnectionsPerIp = value.toInt))))
      case "--max-inflight-requests" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(maxInFlightRequests = value.toInt))))
      case "--request-bytes-per-second" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(requestBytesPerSecond = value.toLong))))
      case "--request-burst-bytes" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(requestBurstBytes = value.toLong))))
      case "--response-bytes-per-second" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(responseBytesPerSecond = value.toLong))))
      case "--response-burst-bytes" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(responseBurstBytes = value.toLong))))
      case "--produce-bytes-per-second" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(produceBytesPerSecond = value.toLong))))
      case "--produce-burst-bytes" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(produceBurstBytes = value.toLong))))
      case "--fetch-bytes-per-second" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(fetchBytesPerSecond = value.toLong))))
      case "--fetch-burst-bytes" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(fetchBurstBytes = value.toLong))))
      case "--max-throttle-ms" :: value :: tail =>
        loop(tail, config.copy(security = config.security.copy(resources = config.security.resources.copy(maxThrottleMillis = value.toLong))))
      case "--operations-host" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(bindHost = value)))
      case "--operations-port" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(port = Some(value.toInt))))
      case "--operations-token-file" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(authenticationToken = Some(readSecret(value)))))
      case "--structured-log" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(structuredLog = Some(Paths.get(value)))))
      case "--structured-log-max-bytes" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(structuredLogMaxBytes = value.toLong)))
      case "--structured-log-retained-files" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(structuredLogRetainedFiles = value.toInt)))
      case "--no-stderr-log" :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(logToStderr = false)))
      case "--readiness-max-pending-flush-bytes" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(readinessMaxPendingFlushBytes = value.toLong)))
      case "--capacity-alert-interval-ms" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(capacityAlerts = config.operations.capacityAlerts.copy(intervalMillis = value.toLong))))
      case "--capacity-connection-ratio" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(capacityAlerts = config.operations.capacityAlerts.copy(connectionUtilization = value.toDouble))))
      case "--capacity-inflight-ratio" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(capacityAlerts = config.operations.capacityAlerts.copy(inFlightUtilization = value.toDouble))))
      case "--capacity-pending-flush-bytes" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(capacityAlerts = config.operations.capacityAlerts.copy(pendingFlushBytes = value.toLong))))
      case "--capacity-minimum-free-bytes" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(capacityAlerts = config.operations.capacityAlerts.copy(minimumFreeBytes = value.toLong))))
      case "--capacity-alert-repeat-ms" :: value :: tail =>
        loop(tail, config.copy(operations = config.operations.copy(capacityAlerts = config.operations.capacityAlerts.copy(repeatIntervalMillis = value.toLong))))
      case "--no-auto-create" :: tail => loop(tail, config.copy(autoCreateTopics = false))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete option: $option")
    val parsed = loop(arguments.toList, BrokerConfig())
    parsed.security.validate()
    parsed.operations.validate()
    parsed

  private def readSecret(value: String): String =
    Files.readString(Paths.get(value), StandardCharsets.UTF_8).stripTrailing()

  private def splitCsv(value: String): Vector[String] =
    value.split(',').iterator.map(_.trim).filter(_.nonEmpty).toVector

  private def parseMappings(value: String): Map[String, String] =
    val entries = splitCsv(value).map { entry =>
      entry.split("=", 2).toList match
        case claim :: role :: Nil if claim.nonEmpty && role.nonEmpty => claim -> role
        case _ => throw IllegalArgumentException(s"invalid OAuth role mapping: $entry")
    }
    if entries.map(_._1).distinct.size != entries.size then throw IllegalArgumentException("duplicate OAuth role mapping")
    entries.toMap

  private def updateOAuth(config: BrokerConfig)(update: OAuthConfig => OAuthConfig): BrokerConfig =
    val authentication = config.security.authentication
    config.copy(security = config.security.copy(authentication = authentication.copy(oauth = update(authentication.oauth))))

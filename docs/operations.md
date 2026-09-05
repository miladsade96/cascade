# Operations runbook

I use this runbook to start, observe, alert on, and stop a Cascade broker. The built-in operations listener is deliberately separate from the Kafka listener and disabled until I pass `--operations-port`.

## Listener boundary

My safe default is `127.0.0.1`. For local collection I start a broker like this:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9092 --advertised-host broker.example.com --data-dir data --operations-port 9404 --structured-log logs/cascade.jsonl --readiness-max-pending-flush-bytes 536870912 --capacity-pending-flush-bytes 536870912 --capacity-minimum-free-bytes 10737418240"
```

If I bind operations to a non-loopback address, Cascade requires a token file containing at least 32 characters:

```powershell
$bytes = [Security.Cryptography.RandomNumberGenerator]::GetBytes(32)
$token = [Convert]::ToHexString($bytes).ToLowerInvariant()
Set-Content -NoNewline operations.token $token
.\sbt.bat "run --host 0.0.0.0 --port 9092 --advertised-host broker.example.com --data-dir data --operations-host 0.0.0.0 --operations-port 9404 --operations-token-file operations.token"
```

The token protects every endpoint and is compared in constant time. It does not provide encryption: the built-in listener is HTTP. I expose it remotely only behind a firewall plus a TLS/mTLS reverse proxy or service mesh, and I restrict the token file with operating-system permissions. I rotate the token with a controlled broker restart because token hot reload is not implemented.

## Endpoint contract

| Endpoint | Healthy response | Failure response | What I use it for |
| --- | ---: | ---: | --- |
| `GET /live` | 200 | 503 | Process supervision; it checks whether the broker is running |
| `GET /ready` | 200 | 503 | Load-balancer admission; it checks running, unfenced state, flush backlog, disk reserve, structured-log health, peer identity-policy reload health, PLAIN/SCRAM/OAuth policy health, and TLS material reload health |
| `GET /metrics` | 200 | 500 | Prometheus 0.0.4 scrape output |
| `GET /v1/status` | 200 | 500 | Compact JSON broker, traffic, storage, disk, and readiness state |

Every response has `Cache-Control: no-store` and `X-Content-Type-Options: nosniff`. Cascade returns 405 for methods other than `GET` and 401 for a missing or invalid bearer token.

For an authenticated request in PowerShell I use:

```powershell
$headers = @{ Authorization = "Bearer $(Get-Content -Raw operations.token)" }
Invoke-RestMethod http://127.0.0.1:9404/ready -Headers $headers
Invoke-WebRequest http://127.0.0.1:9404/metrics -Headers $headers
```

I point Prometheus at the protected endpoint with a secret sourced by my deployment system. A minimal local scrape is:

```yaml
scrape_configs:
  - job_name: cascade
    metrics_path: /metrics
    static_configs:
      - targets: ["127.0.0.1:9404"]
```

Most metric series have only the `node_id` label. SASL success and failure counters add one bounded `mechanism` label with `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, or `UNKNOWN`. Traffic quota metrics use bounded direction/class labels rather than principal names. I export broker uptime/fencing/controller state, topic and local-partition counts, connection/request admission, request and response traffic, cumulative request duration/failures, client and peer authentication/TLS/rejection totals, TLS material generation/reload/failure state, ingress/egress/Produce/Fetch quota activity, flush work/backlog, storage lifecycle/rejection totals, disk capacity, and JVM heap. I keep topic, client, principal, key ID, token, and request identifiers out of labels to avoid unbounded cardinality and secret leakage.

For coordinator scaling I watch `cascade_coordinator_checkpoints_total`, `cascade_coordinator_checkpoint_failures_total`, `cascade_coordinator_checkpoint_seconds_total`, `cascade_coordinator_changed_shards_total`, `cascade_coordinator_delta_bytes_total`, and `cascade_coordinator_full_image_bytes_total`. All six use only `node_id`. Failures include conflicts and unavailable-quorum attempts, not just application errors. I compare the byte counters over the same interval after `coordinator-deltas` activation; they measure proposed delta payloads versus equivalent full coordinator state, not total replication bandwidth. Incremental metadata replication and format-11 shard references reduce physical traffic; full images remain for synchronization and checkpoints.

For [offset batching](offset-batching.md) I compare rates of `cascade_offset_batch_requests_total` and `cascade_offset_batches_total`, alongside pending/peak request and estimated-byte gauges. `cascade_offset_batch_rejected_total` counts pre-admission refusal, while `cascade_offset_batch_failed_total` counts admitted commands ending in errors. My Prometheus rules warn on sustained admission rejection and an admitted failure ratio above ten percent. These rules are inert on older images that do not export the new metrics; deployment image pins remain unchanged. The ratio includes expected retriable conflicts/fencing, so I investigate the cause rather than automatically restarting brokers.

For [controller publication batching](coordinator-publication.md) I compare `cascade_coordinator_publication_committed_requests_total` with `cascade_coordinator_publication_committed_batches_total`. Pending and peak gauges must remain below configured admission limits. Rejections indicate local queue saturation; failures include quorum loss and controller changes; conflicts identify stale terms, shard versions, or invalid payloads. The metrics expose no tenant or shard labels. The metric surface now has 117 fixed series per broker with the currently configured mechanism/quota labels.

I run the repeatable [coordinator scale qualification](coordinator-scaling.md) before release. CI archives its JSON evidence for 1,000 groups, eight concurrent clients, controller failover, and complete broker restart. I investigate sustained retry ratios or checkpoint latency growth before increasing tenant cardinality; I do not use development-machine throughput as a production SLO.

## Readiness policy

I set `--readiness-max-pending-flush-bytes` to the largest dirty backlog I am willing to serve. Its default is effectively unlimited so an operator must choose a deployment-specific threshold. The disk readiness threshold is the larger of `--minimum-free-bytes` and `--capacity-minimum-free-bytes`.

I remove a broker from client routing when `/ready` is 503, but I do not immediately restart it if `/live` remains 200. A fenced broker, flush backlog, or low-disk state often needs the controller, storage, or traffic pressure to recover. I restart only after the failed check and structured events show that the process itself cannot recover.

If `peer_identity_policy` fails, Cascade continues using the last valid policy. I repair and atomically replace the identity file, then wait for readiness to recover. I do not restart into a malformed policy.

If `credential_policy` fails, Cascade likewise continues using the last valid PLAIN, SCRAM, and OAuth JWKS snapshots. I repair and atomically replace the malformed file or restore the verified HTTPS JWKS endpoint, wait for the reload interval, and require readiness plus a test authentication to recover before I finish a credential or signing-key rotation.

If `tls_material` fails, Cascade continues using the last valid key/trust generation for new connections. I atomically restore or repair both stores, wait for readiness to recover, and require a fresh TLS handshake before I finish the rotation. My [TLS rotation runbook](tls-rotation.md) covers leaf and CA replacement without a trust gap.

## Structured events and capacity alerts

With `--structured-log logs/cascade.jsonl`, I write one JSON object per line. I rotate before an event would cross `--structured-log-max-bytes`, retain the configured number of generations, and name them `cascade.jsonl.1`, `cascade.jsonl.2`, and so on. Standard error remains enabled unless I pass `--no-stderr-log`.

The main event families are:

- `broker_starting`, `broker_started`, `broker_stopping`, and `broker_stopped`;
- connection accept, connection handling, protocol, operations-server, storage, and capacity-monitor failures;
- `peer_authentication` audit events for certificate-bound internal requests, with allowed or denied decisions;
- `tls_material_reloaded` and `tls_material_reload_failed` for atomic key/trust rotation outcomes;
- `capacity_alert` with `alert`, `current`, `threshold`, and `unit` fields;
- `capacity_alert_resolved` when an active condition clears.

I evaluate capacity immediately after startup and then every `--capacity-alert-interval-ms`. Connection and request thresholds are ratios of their configured hard limits. Pending flush and minimum free disk are byte thresholds; zero disables that byte alert. I repeat a continuing alert only after `--capacity-alert-repeat-ms`, so my log or external collector is not flooded.

For non-Kubernetes deployments I route these events through my log collector and close the incident on `capacity_alert_resolved`. The Kubernetes pack also includes `PrometheusRule` alerts for broker loss/fencing, controller disagreement, disk/heap/flush pressure, storage rejection, request failures, peer-authentication rejection, TLS reload failure, and quota rejection. A Grafana dashboard ConfigMap covers availability, traffic, latency, storage, quotas, JVM, and security signals.

## Kafka Admin visibility

Kafka Admin 4.3.1 can call `describeConfigs` for `ConfigResource.Type.BROKER` and `ConfigResource.Type.TOPIC`. I return only non-sensitive effective values. Broker defaults remain read-only; topic `cleanup.policy`, `retention.ms`, and `retention.bytes` can be set, deleted back to the broker default, or appended/subtracted where the configuration type allows it through `IncrementalAlterConfigs` v0. Cascade validates the full resulting policy, commits it through the metadata quorum, and exposes it only after the commit succeeds.

## Kubernetes monitoring pack

I render the production topology with `kubectl kustomize deploy/kubernetes`. The operations Service is cluster-local, the bearer token comes from the `cascade-operations` Secret, and the `ServiceMonitor` sends that token on `/metrics`. I install the Prometheus Operator CRDs before applying the pack because `ServiceMonitor` and `PrometheusRule` are custom resources.

The generated `cascade-grafana-dashboard` ConfigMap has the `grafana_dashboard=1` discovery label. My Grafana sidecar imports it automatically when configured for that label. I keep alert routing, receivers, silences, and escalation policy in the platform Alertmanager configuration because those values contain organization-specific endpoints and secrets.

## Startup and shutdown checklist

Before startup I verify that the Kafka and operations addresses are correct, the operations token and TLS-proxy secret are readable only by the service identity, the structured-log volume has enough space, and the data/backup paths are on the intended devices. I then wait for `/live` and `/ready`, confirm a successful Prometheus scrape, check the controller/fenced fields, and verify that `broker_started` names both bound ports.

In a container I use the built-in `cascade.operations.ContainerHealthCheck`, which reads `CASCADE_OPERATIONS_PORT`, `CASCADE_HEALTHCHECK_HOST`, `CASCADE_HEALTHCHECK_TIMEOUT_MS`, and the optional `CASCADE_HEALTHCHECK_TOKEN_FILE`. I leave the operations listener on container loopback for internal health only. If I publish it for metrics, I bind it explicitly, mount the token file read-only, point the health check to that file, and retain the same TLS boundary described above. The full non-root, read-only-root, volume, Compose, and image-release procedure is in my [container deployment runbook](containers.md).

For shutdown I first drain client traffic, wait for in-flight work and pending flush bytes to fall, terminate the broker normally, and wait for `broker_stopped`. A clean close forces dirty logs and publishes the clean-shutdown marker required by the backup tool. I never take an offline backup after a forced kill until I have started Cascade to perform recovery and then completed a clean shutdown.

My [TLS rotation runbook](tls-rotation.md) covers listener and cluster PKI replacement. My [OAuth and OIDC runbook](oauth-oidc.md) covers provider policy, client configuration, signing-key rotation, and token monitoring. My [SCRAM authentication runbook](scram-authentication.md) covers verifier generation, client configuration, rotation, and authentication monitoring. My [broker-to-broker security runbook](peer-security.md) covers certificate inventory, identity policy, monitoring, and live rotation. My separate [backup and restore runbook](backup-restore.md) covers disaster-recovery copies and restore drills. The remaining release gates stay in [production-readiness.md](production-readiness.md).

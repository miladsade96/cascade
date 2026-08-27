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
| `GET /ready` | 200 | 503 | Load-balancer admission; it checks running, unfenced state, flush backlog, disk reserve, structured-log health, peer identity-policy reload health, and PLAIN/SCRAM credential-policy reload health |
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

Most metric series have only the `node_id` label. SASL success and failure counters add one bounded `mechanism` label with `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, or `UNKNOWN`. I export broker uptime/fencing/controller state, topic and local-partition counts, connection/request admission, request and response traffic, cumulative request duration/failures, client and peer authentication/TLS/rejection totals, quota activity, flush work/backlog, storage lifecycle/rejection totals, disk capacity, and JVM heap. I keep topic, client, principal, and request identifiers out of labels to avoid unbounded cardinality.

## Readiness policy

I set `--readiness-max-pending-flush-bytes` to the largest dirty backlog I am willing to serve. Its default is effectively unlimited so an operator must choose a deployment-specific threshold. The disk readiness threshold is the larger of `--minimum-free-bytes` and `--capacity-minimum-free-bytes`.

I remove a broker from client routing when `/ready` is 503, but I do not immediately restart it if `/live` remains 200. A fenced broker, flush backlog, or low-disk state often needs the controller, storage, or traffic pressure to recover. I restart only after the failed check and structured events show that the process itself cannot recover.

If `peer_identity_policy` fails, Cascade continues using the last valid policy. I repair and atomically replace the identity file, then wait for readiness to recover. I do not restart into a malformed policy.

If `credential_policy` fails, Cascade likewise continues using the last valid PLAIN and SCRAM snapshots. I repair and atomically replace the malformed file, wait for the reload interval, and require readiness plus a test authentication to recover before I finish a credential rotation.

## Structured events and capacity alerts

With `--structured-log logs/cascade.jsonl`, I write one JSON object per line. I rotate before an event would cross `--structured-log-max-bytes`, retain the configured number of generations, and name them `cascade.jsonl.1`, `cascade.jsonl.2`, and so on. Standard error remains enabled unless I pass `--no-stderr-log`.

The main event families are:

- `broker_starting`, `broker_started`, `broker_stopping`, and `broker_stopped`;
- connection accept, connection handling, protocol, operations-server, storage, and capacity-monitor failures;
- `peer_authentication` audit events for certificate-bound internal requests, with allowed or denied decisions;
- `capacity_alert` with `alert`, `current`, `threshold`, and `unit` fields;
- `capacity_alert_resolved` when an active condition clears.

I evaluate capacity immediately after startup and then every `--capacity-alert-interval-ms`. Connection and request thresholds are ratios of their configured hard limits. Pending flush and minimum free disk are byte thresholds; zero disables that byte alert. I repeat a continuing alert only after `--capacity-alert-repeat-ms`, so my log or external collector is not flooded.

I currently route alerts through structured events. I still need a standard Alertmanager/webhook integration and maintained dashboard pack, so I configure my log collector to page on `capacity_alert` and close the incident on `capacity_alert_resolved`.

## Kafka Admin visibility

Kafka Admin 4.3.1 can call `describeConfigs` for `ConfigResource.Type.BROKER` and `ConfigResource.Type.TOPIC`. I return only non-sensitive effective values and mark them read-only. Topic values currently reflect broker-wide defaults because per-topic lifecycle/config mutation is not implemented. I do not advertise `AlterConfigs` or `IncrementalAlterConfigs`.

## Startup and shutdown checklist

Before startup I verify that the Kafka and operations addresses are correct, the operations token and TLS-proxy secret are readable only by the service identity, the structured-log volume has enough space, and the data/backup paths are on the intended devices. I then wait for `/live` and `/ready`, confirm a successful Prometheus scrape, check the controller/fenced fields, and verify that `broker_started` names both bound ports.

For shutdown I first drain client traffic, wait for in-flight work and pending flush bytes to fall, terminate the broker normally, and wait for `broker_stopped`. A clean close forces dirty logs and publishes the clean-shutdown marker required by the backup tool. I never take an offline backup after a forced kill until I have started Cascade to perform recovery and then completed a clean shutdown.

My [SCRAM authentication runbook](scram-authentication.md) covers verifier generation, client configuration, rotation, and authentication monitoring. My [broker-to-broker security runbook](peer-security.md) covers certificate inventory, identity policy, monitoring, and rolling rotation. My separate [backup and restore runbook](backup-restore.md) covers disaster-recovery copies and restore drills. The remaining release gates stay in [production-readiness.md](production-readiness.md).

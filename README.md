# Cascade

<p align="center">
  <img src="docs/assets/cascade-logo.png" alt="Cascade logo: high-speed cascading data streams" width="960">
</p>

> I'm building a fast Kafka-style streaming log in pure Scala 3.

I built Cascade around the Kafka wire protocol so existing Kafka clients can connect without a custom SDK. If a language has a client that speaks one of the supported protocol versions, it can talk to Cascade.

The broker itself only needs Scala and the JDK. I use Apache Kafka's Java client in the test suite as an independent compatibility check; it isn't a runtime dependency.

The development version is `1.3.0-SNAPSHOT`; the last qualified local image is `1.2.0`. I use the root `VERSION` file for broker capabilities and build metadata. The Dockerfile and compatibility package follow the development version; deployment examples remain pinned to the previously tested 1.2.0 image. That image does not include shard-object storage or bounded coordinator batching, and neither tag has been published to Docker Hub by these milestones.

So far, I've implemented broker-assigned offsets, magic-v2 record batches, classic and server-assigned consumer coordination, durable metadata and offset journals, idempotent producer recovery, transactions, `read_committed` isolation, ISR replication, partition-leader promotion, quorum controller election, rendezvous-sharded coordinator ownership, coordinator failover, online partition reassignment, dynamic broker/voter membership, rolling feature negotiation, crash-safe storage lifecycle management, record-level/gzip compaction with tombstone grace and cleanup throttling, TLS, Kafka PLAIN, SCRAM-SHA-256/512, OAUTHBEARER, Kafka ACL Admin APIs, security auditing, conservative cluster-shared principal quotas, Prometheus metrics, health/readiness checks, structured events, Kubernetes artifacts, capacity alerts, offline backup/restore, and write-barrier online snapshots.

> [!IMPORTANT]
> Cascade isn't a production Kafka replacement yet. The code has peer capability negotiation, a negotiated metadata-format floor, coordinator sharding with incremental replication and immutable shard-object persistence, Kafka's ConsumerGroupHeartbeat v0 path, Metadata v4-v12 discovery with v10-v12 topic IDs, online point-in-time broker snapshots, record-level/gzip compaction, conservative cluster-shared quotas, and repeatable soak/power-loss probes. Pinned 1.0.0, format-9, and format-10 source rolling campaigns against 1.3.0-SNAPSHOT pass, including pre-activation rollback and unsafe-downgrade rejection. The published format-8 image pair, multi-day soak, and physical power/device-loss campaign remain unqualified. A cluster-wide snapshot still needs one artifact from every replica host, and compaction for Snappy/LZ4/Zstd batches is still conservative.

## Performance I measured

The latest coordinator increment adds [bounded offset commit batching](docs/offset-batching.md), atomic commit validation/read isolation, and a fix for readiness changes producing false missing-offset responses. Matched 1,000-group persistent-client campaigns passed exact offsets through controller failover and full restart at both 8 and 32 workers. Batching measured **24.480 vs 20.302 writes/s** at 8 workers and **41.922 vs 26.137 writes/s** at 32 workers, with zero batch/connection admission rejections. These are single runs on my development host, not production SLOs; the 32-worker batched p99 is still **6.342 seconds**. Independent shard consensus, service-lock removal, and full-state CPU optimization remain capacity gates.

I measured this on my Windows development machine with Java 21, eight partitions, four producers, four consumers, 1 KiB deterministic incompressible payloads, LZ4, `acks=all`, and periodic background flushing:

| Workload | Produce | Consume | Exact verification |
| --- | ---: | ---: | ---: |
| 10,000,000 records, OAuth/OIDC milestone regression | **368,387 records/s** / **359.8 MiB/s** | **351,784 records/s** / **343.5 MiB/s** | **10,000,000 / 10,000,000** |
| 10,000,000 records, SCRAM milestone regression | **333,835 records/s** / **326.0 MiB/s** | **349,851 records/s** / **341.7 MiB/s** | **10,000,000 / 10,000,000** |
| 10,000,000 records, secure-peer milestone regression | **350,319 records/s** / **342.1 MiB/s** | **329,303 records/s** / **321.6 MiB/s** | **10,000,000 / 10,000,000** |
| 10,000,000 records, sustained | **182,285 records/s** / **178.0 MiB/s** | **473,058 records/s** | **10,000,000 / 10,000,000** |
| 1,000,000 records, calibration | **614,413 records/s** / **600.0 MiB/s** | **556,232 records/s** / **543.2 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, coordinator-failover regression | **510,714 records/s** / **498.7 MiB/s** | **263,637 records/s** / **257.5 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, fault-qualification regression | **503,499 records/s** / **491.7 MiB/s** | **250,190 records/s** / **244.3 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, storage-lifecycle regression | **423,662 records/s** / **413.7 MiB/s** | **477,368 records/s** / **466.2 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, security/isolation regression | **544,611 records/s** / **531.8 MiB/s** | **291,753 records/s** / **284.9 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, operations/recovery regression | **533,937 records/s** / **521.4 MiB/s** | **263,944 records/s** / **257.8 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, compatibility/release regression | **441,383 records/s** / **431.0 MiB/s** | **489,195 records/s** / **477.7 MiB/s** | **1,000,000 / 1,000,000** |

After I fixed the background-flush path, sustained ten-million-record production went from 57,400 to 182,285 records/s: a **3.18x improvement**. The write phase dropped from 174.2 to 54.9 seconds. That run forced 9.58 GiB in 47.6 cumulative seconds, so the drive was the main limit on this machine.

After the secure-peer milestone, I reran all ten million records on 2026-08-24. Produce finished in 28.545 seconds at 350,319 records/s and consume finished in 30.367 seconds at 329,303 records/s. The run stored 9.86 GiB, forced 9.67 GiB during production, reached 3,636.325 ms maximum acknowledgement latency, and used 5,336.7 MiB peak heap. The load harness is single-node plaintext, so I use this result as an exactness and default-data-path regression gate rather than a benchmark of TLS or replicated capacity.

After the SCRAM milestone, I ran the same ten-million-record workload on 2026-08-27. Produce finished in 29.955 seconds at 333,835 records/s and consume finished in 28.584 seconds at 349,851 records/s. It verified every record, stored 9.86 GiB, forced 9.45 GiB during production, reached 3,989.008 ms maximum acknowledgement latency, and used 5,387.4 MiB peak heap. This harness still uses the default single-node plaintext listener, so it qualifies exactness and catches inactive-authentication regressions; it does not measure SCRAM, TLS, or replicated-cluster capacity.

After the OAuth/OIDC milestone, I repeated all ten million records on 2026-08-30. Produce finished in 27.145 seconds at 368,387 records/s and consume finished in 28.427 seconds at 351,784 records/s. It verified every record, stored 9.86 GiB, forced 9.48 GiB during production, reached 3,213.600 ms maximum acknowledgement latency, and used 5,391.9 MiB peak heap. This remains a single-node plaintext default-path regression, not an OAuth, TLS, or replicated-cluster capacity measurement.

After closing the multi-language compatibility gate, I ran the one-million regression on 2026-08-31. It produced 441,383 records/s, consumed 489,195 records/s, and verified exactly 1,000,000 / 1,000,000 records. Maximum acknowledgement latency was 741.467 ms, 749.3 MiB was forced in 11 operations during production, and peak heap was 1,702.6 MiB. I use this short cache-assisted run as an exactness and default-path regression check.

The one-million test is much shorter and benefits a lot from the filesystem cache. I don't present either result as production capacity. The [full heavy-load report](docs/performance/2026-08-05-heavy-load.md) includes the machine, workload, latency, CPU, GC, heap, storage, and test method.

## What I'm building

| Area | What is implemented |
| --- | --- |
| Language-neutral access | Length-prefixed Kafka TCP frames and an explicit `ApiVersions` contract; no custom client library required |
| Pure Scala/JDK runtime | Scala 3 broker implementation with Java 21 virtual threads and positional file I/O |
| Efficient record path | Kafka magic-v2 batches remain compressed and opaque; the broker updates only the base offset outside the batch CRC region |
| Delivery guarantees | Producer IDs, epoch fencing, bounded duplicate detection, sequence recovery, transactions, timeouts, transactional offsets, and `read_committed` |
| Durable state | CRC32C-protected local journals plus one versioned, quorum-committed coordinator image for groups, offsets, producers, and transactions |
| Consumer coordination | Classic join, sync, heartbeat, leave, rebalance, session expiry, and durable committed offsets |
| Dynamic cluster | Durable joint-consensus membership, Kafka Admin add/remove/describe APIs, controller election and fencing, synchronous ISR replication, persisted committed high watermarks, leader promotion, incremental divergent-tail repair, and safe replica re-admission |
| Failure qualification | Deterministic directional partitions and protocol-triggered drops, subprocess force kills, clean/unclean startup detection, torn-tail recovery, and stable/joint quorum safety checks |
| Storage lifecycle | Scheduled time/size retention, conservative keyed compaction, offset expiry, bounded coordinator journals, batch timestamp/transaction indexes, atomic retirement, and disk-reserve admission |
| Security and isolation | TLS 1.2/1.3, Kafka PLAIN, SCRAM-SHA-256/512, and OAUTHBEARER, offline password verifiers, RSA/EC/Ed25519 JWKS validation, approved role mapping, deny-by-default ACLs and Kafka ACL Admin APIs, JSONL audit events, hostname-verified peer mTLS, atomic secret/policy rotation, connection/request caps, directional principal quotas, and overload shedding |
| Operations and recovery | Separate health/readiness/status endpoints, Prometheus 0.0.4 metrics, rotating structured events, deduplicated capacity alerts, mutable per-topic Kafka configuration, checksummed offline backup/restore, hardened containers, Kubernetes StatefulSets, NetworkPolicies, disruption budgets, Prometheus rules, and a Grafana dashboard |
| Measured performance | Repeatable one-million and ten-million tests with exact record counting, latency, CPU, GC, heap, storage, and flush metrics |

## What works now

### Kafka-compatible networking

- Persistent, length-delimited, big-endian Kafka TCP frames.
- Hard request-size bounds and version validation before request handling.
- `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` listeners with TLS 1.2/1.3 and optional client-certificate verification.
- Atomic key/trust-store reload for new client handshakes plus generation-aware peer reconnection; bad replacements preserve the last valid context and fail readiness.
- Internal controller, metadata, replication, and recovery requests can require hostname-verified mutual TLS plus a certificate subject assigned to the claimed node ID.
- Kafka-framed `SaslHandshake` v1 and `SaslAuthenticate` v1 with PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, and OAUTHBEARER identities scoped to one connection.
- Global/per-IP connection caps, a bounded global in-flight request gate, and independent per-principal ingress, egress, Produce, and Fetch token buckets with Kafka throttle fields.
- Correlation IDs preserved in every response.
- Ordered processing within a connection and Java 21 virtual-thread isolation between connections.
- Explicitly advertised API keys and versions rather than a broad, unverified compatibility claim.
- Kafka 4.3.1 Admin, Producer, transactional Producer, explicit Consumer, and subscribed Consumer interoperability tests.
- External exact-delivery tests for KafkaJS 2.2.4, confluent-kafka Python 2.15.0, franz-go 1.21.0, and Confluent.Kafka .NET 2.15.0.

Any language can connect if its client speaks one of the supported Kafka protocol versions. I run the external [client compatibility matrix](compatibility/README.md) independently of the Scala test process and fail CI if any client sees the wrong records, cannot commit offsets, or causes a broker-side protocol error.

### Storage and durability

- Immutable Kafka magic-v2 record batches stored without broker-side decompression or recompression.
- Client compression remains compatible with Snappy, LZ4, Zstd, and gzip because payloads stay opaque to storage.
- Serialized broker offset assignment per partition.
- Batch-aligned Fetch responses and correct first-batch size behavior.
- Size-based log segments and startup index reconstruction by scanning batch headers.
- Incomplete-tail truncation and removal of later segments whose offsets depend on a damaged earlier tail.
- `periodic` background flushing with dirty-age, byte-threshold, and segment-rollover triggers.
- `sync` flushing for strict per-append local persistence.
- Clean shutdown forces all remaining dirty segments.
- Scheduled time and per-partition size retention only retire closed, fully committed segments and preserve the active segment.
- Segment retirement uses an atomic `.deleted` rename; startup finishes an interrupted deletion without exposing the retired log again.
- Conservative keyed compaction rewrites closed segments atomically and removes a batch only when every key has a strictly newer offset.
- Compaction keeps compressed, malformed, keyless, control, and transactional batches opaque and untouched.
- Batch timestamp and transaction-range indexes rebuild from immutable headers at startup; `ListOffsets` now returns the first batch whose maximum timestamp reaches the requested time.
- Offset expiry is committed through the same local or quorum coordinator checkpoint as ordinary offset changes.
- Offset, delivery, and cluster-metadata journals compact to their latest checksum-protected image after a configurable byte threshold.
- A configurable free-space reserve rejects an append before its log end changes and returns Kafka error 56 (`KAFKA_STORAGE_ERROR`).
- Per-topic cleanup and retention policy is committed through the metadata quorum, changed through `IncrementalAlterConfigs` v0, and restored after controller loss.

Kafka acknowledgements and local disk forcing are separate settings. In single-node periodic mode, `acks=1` and `acks=all` acknowledge a local append before the next scheduled force. A process, OS, or power failure before that force can lose those records. In cluster mode, `acks=all` requires the configured minimum ISR and waits for every current ISR member to append before the committed high watermark advances.

### Idempotence and transactions

In single-node and clustered modes I support this Kafka delivery-semantics path:

- Durable producer ID allocation and producer-epoch fencing.
- Per-partition sequence validation with wraparound after `Int.MaxValue`.
- A bounded five-batch duplicate window that returns the original offset for a retry.
- O(1) lookup of recent producer batches on the append hot path.
- Producer sequence recovery from persisted record-batch headers after restart.
- Durable active transaction, outcome, range, timeout, and transactional-offset state.
- Transaction commit and abort, including safe coordination with in-flight appends.
- Last stable offsets and `read_committed` filtering of aborted or open transactions.
- Transaction timeouts and automatic abort when a new epoch fences an old owner.
- Applied checkpoints that recover an interrupted transactional offset commit without replaying old transactions over newer offsets.

In cluster mode I commit producer registration, fencing epochs, active transaction ranges, outcomes, and transactional consumer offsets as one atomic coordinator image. After every voter activates `coordinator-deltas`, proposals carry only changed virtual shards and compare against the shard versions actually installed locally. Independent shards can advance from the same global starting image; a conflict in any touched shard rejects the whole transaction. Producer-ID allocation stays serialized in its own shard. If the quorum rejects a transition, I restore authoritative state and return a retriable coordinator error. My Kafka-client test commits an open transaction and its staged offset after the original coordinator stops, then verifies `read_committed` visibility and successor producer initialization.

### Consumer groups

- Classic Kafka group protocol with member ID allocation.
- Join and sync phases, protocol selection, and leader-provided assignments.
- Heartbeats, session expiration, explicit leave, and rebalance initiation.
- Generation/member validation for ordinary offset commits.
- Append-only CRC32C-protected offset journal with one forced commit per multi-partition request.
- Offset recovery across restart and partial/corrupt-tail truncation.
- Transactional offset staging and commit through `TxnOffsetCommit`.
- Quorum snapshots of group generations, members, assignments, pending identities, and committed offsets.
- Controller-term ownership, stale-image rejection, and Kafka-client continuation after coordinator failover.
- Static `group.instance.id` ownership with duplicate-instance fencing across joins and heartbeats.

If a client exposes Kafka's newer consumer protocol, set `group.protocol=classic` for now.

### Dynamic replicated cluster

- Majority-committed, monotonic metadata images.
- Durable controller terms and one vote per term in a forced CRC32C journal.
- Majority election with metadata-freshness voting, randomized retry deadlines, and a preferred initial candidate.
- Controller heartbeats and leases that fence isolated or not-yet-synchronized brokers.
- A committed controller-term image that bumps every partition leader epoch after election.
- Dynamic controller discovery in Kafka Metadata and coordinator responses.
- Round-robin replica assignment and configurable default replication factor.
- Partition leaders, leader epochs, replica sets, and ISR state.
- Parallel synchronous leader-to-follower append.
- `min.insync.replicas` admission for `acks=all`.
- Leader-only Fetch/ListOffsets and committed high-watermark visibility.
- Double-buffered, checksum-protected high-watermark checkpoints that never recover beyond the validated log end.
- Failure detection, ISR shrink, and promotion of a surviving replica with a new leader epoch.
- Chained SHA-256 prefix probes that find a returning replica's last verified common batch boundary.
- Bounded, configurable suffix transfer that preserves the shared prefix and replaces only a divergent or missing tail.
- A short final-delta fence so Produce cannot race ISR admission after the online bulk copy.
- ISR re-admission only after catch-up succeeds and the new metadata image reaches quorum.
- Durable online reassignment state with learners outside the ISR, add-before-remove ordering, replacement plans, and cancellation.
- Bulk learner catch-up without blocking Produce, followed by a short final-delta fence and atomic target-assignment commit.
- Kafka Admin `AlterPartitionReassignments` and `ListPartitionReassignments` compatibility, including controller-failover resume.
- Persisted voter endpoints and directory IDs with backward-compatible bootstrap from the configured initial quorum.
- Stable → joint → stable membership changes that require majorities from both competing voter sets for elections, leases, and metadata commits.
- Observer discovery and committed-metadata synchronization before a broker can become a voter.
- Kafka Admin `DescribeMetadataQuorum`, `AddRaftVoter`, and `RemoveRaftVoter` compatibility.
- One membership change at a time, partition-drain enforcement before removal, automatic completion after controller failover, and active-controller handoff.
- Rollback of uncommitted `acks=all` appends so a transient replica miss cannot poison the next retry's base offset.
- Real Kafka-client end-to-end verification across partition-leader loss, controller loss, stale-term rejection, metadata creation after election, and broker restart/rejoin.
- One atomic, versioned coordinator image replicated by the metadata quorum and installed on every synchronized broker.
- Optimistic per-shard fencing for delta proposals, with whole-image fencing retained during mixed-version operation.
- Controller-only group/transaction expiration so a follower timer cannot overwrite live coordinator state.
- Injectable peer transports with deterministic directional API drops and partitions for repeatable fault qualification.
- Subprocess force-kill recovery tests that distinguish clean and unclean startup without relying on shutdown hooks.
- Exact data, transaction, and committed-offset recovery after a forced JVM kill, including conservative truncation of torn data and coordinator journal tails.
- Majority availability through an active-controller partition, minority coordinator fencing, durable joint-transition resume, joint-controller loss, and rejection of metadata writes unless both joint voter majorities are present.

The configured node list bootstraps the first committed voter set and gives observers discovery endpoints. After that, the committed metadata image is authoritative. Recovery is incremental at Kafka batch boundaries; only its final delta and ISR admission are partition-fenced. Coordinator mutations are acknowledged only after a quorum forces the atomic group/offset/producer/transaction change. I route keys with rendezvous hashing, isolate conflicts across 64 group shards, 64 transaction shards, and one allocator shard, and coalesce asynchronous installation to one pending image. With `incremental-coordinator` active, consecutive coordinator-only changes use delta journal records and peer commits; structural changes, checkpoints, and missing-base recovery use complete images. Publication and in-memory state remain shared: this is not Kafka-style independent internal-topic consensus. I document the boundary in [coordinator scaling](docs/coordinator-scaling.md) and [incremental persistence](docs/incremental-coordinator.md).

### Operations and disaster recovery

- I expose `/live`, `/ready`, `/metrics`, and `/v1/status` on a separate listener that is disabled by default and bound to loopback by default.
- Readiness checks broker state, fencing, pending flush bytes, disk reserve, and structured-log health. Liveness only answers whether the broker is running.
- Prometheus output uses bounded broker-level labels and includes traffic, request duration, connection/admission, quota, flush, lifecycle, disk, and JVM heap measurements.
- My JSONL operational events rotate by size and include broker lifecycle, protocol/connection failures, storage failures, and capacity alert/resolution events.
- Capacity checks cover connection and in-flight request utilization, pending flush bytes, and usable disk. I deduplicate an active alert until its repeat interval and emit a resolution when it clears.
- Kafka Admin can read broker/topic configuration and atomically alter supported per-topic cleanup and retention values through `IncrementalAlterConfigs` v0.
- My maintenance commands create, verify, and restore an offline backup with an exact manifest, SHA-256 per file, forced destination files, and atomic publication. A running broker can also take a point-in-time local snapshot behind an exclusive write barrier. Restore refuses an existing target and verifies every copied file before publication.
- My Kubernetes manifests provide a three-broker StatefulSet topology, headless/client/operations services, pod anti-affinity, a disruption budget, default-deny network policy, secret-backed monitoring authentication, a ServiceMonitor, Prometheus alert rules, and a Grafana dashboard.

I keep the operations listener on loopback unless a protected monitoring network or TLS-terminating proxy is in front of it. A bearer token authenticates a remote listener, but the built-in HTTP listener does not encrypt that token. Online snapshots are point-in-time local broker artifacts; I still quiesce the cluster for a provably coordinated multi-node recovery set. The [operations runbook](docs/operations.md) and [backup/restore runbook](docs/backup-restore.md) contain the deployment and recovery procedure.

## Run Cascade

### Requirements

- JDK 21+
- Network access to Maven Central for the first build

The included launchers download SBT the first time you run them.

### Run the container

I can start the hardened single-broker Compose deployment without installing Java or SBT on the host:

```bash
docker compose up --build --detach
docker compose ps
```

The image runs as non-root UID 65532 with a read-only root filesystem, an internal readiness check, cgroup-aware JDK 21 memory settings, and a named volume at `/var/lib/cascade`. Kafka is available at `localhost:9092`; `docker compose down` stops cleanly and preserves the data volume. I use `compose.cluster.yaml` for a three-broker local cluster on ports 19092 through 19094. Pulling the released image, authenticated monitoring, immutable tags, multi-architecture publishing, volume ownership, and the Kafka-client smoke procedure are in my [container deployment runbook](docs/containers.md).

### Run the tests and start one broker

Windows:

```powershell
.\sbt.bat test
.\sbt.bat "run --host 0.0.0.0 --port 9092 --advertised-host localhost --data-dir data"
```

Linux/macOS:

```bash
chmod +x sbt
./sbt test
./sbt "run --host 0.0.0.0 --port 9092 --advertised-host localhost --data-dir data"
```

Point any supported Kafka client at `localhost:9092`. A basic single-node producer configuration is:

```properties
bootstrap.servers=localhost:9092
acks=all
enable.idempotence=true
```

For a consumer using subscriptions and committed-only visibility:

```properties
bootstrap.servers=localhost:9092
group.protocol=classic
isolation.level=read_committed
```

Set `transactional.id` on a producer when you want to use transactions. Use `read_uncommitted` only when the consumer should also see aborted transactional records.

### Enable operational endpoints

I keep the listener local for a development broker and write rotating events to a separate directory:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9092 --advertised-host localhost --data-dir data --operations-port 9404 --structured-log logs/cascade.jsonl --readiness-max-pending-flush-bytes 536870912 --capacity-pending-flush-bytes 536870912 --capacity-minimum-free-bytes 10737418240"
Invoke-WebRequest http://127.0.0.1:9404/ready
Invoke-WebRequest http://127.0.0.1:9404/metrics
```

For a non-loopback operations host I provide `--operations-token-file` with at least 32 characters and put TLS in front of the listener. I never put that token directly on the command line. The full endpoint contract, Prometheus scrape example, alert behavior, and startup/shutdown procedure are in my [operations runbook](docs/operations.md).

### Back up and restore one stopped broker

After the broker has completed a clean shutdown, I run:

```powershell
.\sbt.bat "run backup --data-dir data --backup-dir backups/cascade-2026-08-24"
.\sbt.bat "run verify-backup --backup-dir backups/cascade-2026-08-24"
.\sbt.bat "run restore --backup-dir backups/cascade-2026-08-24 --data-dir restored-data"
```

The backup directory and restore target must not already exist. I use the [backup/restore runbook](docs/backup-restore.md) for clustered backups, retention, off-host copies, and restore drills.

### Protect a client listener

I use `SASL_SSL` because SASL authenticates the client but does not encrypt application traffic. I prefer SCRAM-SHA-512 for password authentication and keep the key-store password in a separate file instead of a command-line argument.

I generate an offline salted SCRAM verifier like this, then copy the final `SCRAM-SHA-512 alice=scram-sha-512$...` line into `scram-users.conf`. Cascade never needs the cleartext password:

```powershell
[IO.File]::WriteAllText('alice.password', 'replace-with-a-long-random-secret', [Text.UTF8Encoding]::new($false))
.\sbt.bat "runMain cascade.security.CredentialTool alice --password-file alice.password --mechanism SCRAM-SHA-512"
Remove-Item -LiteralPath alice.password
```

My ACL file uses `effect principal operation resource-type resource-pattern`. A final `*` makes a prefix rule, a rule containing only `*` matches everything, and an explicit deny wins over every matching allow:

```text
allow alice Create Topic orders
allow alice Describe Topic orders
allow alice Write Topic orders
allow alice Read Topic orders
allow alice Read Group order-workers
allow alice Describe Group order-workers
deny alice Write Topic orders-private
```

I start the protected listener with a PKCS12 or JKS key store:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9093 --advertised-host broker.example.com --data-dir data --security-protocol SASL_SSL --ssl-keystore broker.p12 --ssl-keystore-password-file broker-store.password --sasl-mechanisms SCRAM-SHA-512 --scram-credentials-file scram-users.conf --acl-file acls.conf --audit-log security-audit.jsonl --max-connections 10000 --max-connections-per-ip 1000 --max-inflight-requests 10000 --request-bytes-per-second 104857600 --request-burst-bytes 209715200 --max-throttle-ms 1000"
```

The matching Kafka client properties are:

```properties
bootstrap.servers=broker.example.com:9093
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="replace-with-a-long-random-secret";
ssl.truststore.location=cluster-ca.p12
ssl.truststore.password=replace-with-the-truststore-password
ssl.truststore.type=PKCS12
group.protocol=classic
```

I can enable a migration set such as `--sasl-mechanisms PLAIN,SCRAM-SHA-256,SCRAM-SHA-512`; in that case I provide both `--credentials-file` and `--scram-credentials-file`. TLS material, credentials, ACLs, and peer identities reload on their configured intervals. A malformed replacement never replaces the last valid in-memory snapshot; `tls_material` or `credential_policy` fails readiness until I repair it. My [TLS rotation runbook](docs/tls-rotation.md) covers listener/cluster PKI changes, and my [SCRAM authentication runbook](docs/scram-authentication.md) covers verifier generation, migration, rotation, metrics, and limitations.

### Authenticate with OAuth or OIDC

For service identities, I can require signed JWT access tokens issued by an OAuth 2.0 or OpenID Connect provider. Cascade requires TLS, fetches an explicit JWKS over verified HTTPS (or reads an atomically managed `file:` URI), and validates RSA, ECDSA, or Ed25519 signatures, `kid`, algorithm allowlist, issuer, audience, expiry, activation time, principal, required scopes, and optional operator-approved role mappings:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9093 --advertised-host broker.example.com --data-dir data --security-protocol SASL_SSL --ssl-keystore broker.p12 --ssl-keystore-password-file broker-store.password --sasl-mechanisms OAUTHBEARER --oauth-jwks-uri https://identity.example.com/oauth2/keys --oauth-issuer https://identity.example.com --oauth-audience cascade --oauth-required-scopes cascade.read,cascade.write --oauth-allowed-algorithms RS256,ES256,EdDSA --oauth-role-claim roles --oauth-role-map orders-writer=orders-write,orders-reader=orders-read --acl-file acls.conf --audit-log security-audit.jsonl"
```

Kafka 4.3.1 clients can use `OAuthBearerLoginModule` with their provider's token endpoint, built-in JWT retriever, or a custom login callback. Cascade never receives the client secret; it receives only the bearer token inside TLS. A bad JWKS refresh preserves the last valid keys and fails `credential_policy` readiness. My [OAuth and OIDC runbook](docs/oauth-oidc.md) documents the provider contract, Kafka client settings, overlapping key rotation, monitoring, bounds, and current limitations.

### Protect broker-to-broker traffic

I give every node a distinct CA-signed certificate whose SAN matches its advertised host. My peer identity file binds each node ID to its canonical X.500 certificate subject:

```text
1 CN=cascade-1,OU=Production,O=Example Corp
2 CN=cascade-2,OU=Production,O=Example Corp
3 CN=cascade-3,OU=Production,O=Example Corp
```

I enable `SSL` or `SASL_SSL`, configure the cluster CA trust store, request or require client certificates, and add these peer options to every broker:

```text
--peer-security-protocol SSL
--peer-identity-file secrets/peer-identities.conf
--peer-identity-reload-ms 1000
```

Cascade verifies the trust chain and advertised hostname during connection setup, then rejects an internal request unless the certificate subject is assigned to the node ID in `cascade-peer:<node-id>`. I can overlap old/new subjects and CA trust, atomically replace stores, and let peer channels reconnect on the new TLS generation without restarting a broker. The full deployment, monitoring, and rotation procedure is in my [broker-to-broker security runbook](docs/peer-security.md).

### Bootstrap a three-node cluster for development

Run each command in a separate process and give every broker its own data directory. These short commands intentionally use plaintext for local development; I add the peer TLS settings above for any non-local deployment:

```powershell
.\sbt.bat "run --host 127.0.0.1 --port 9092 --advertised-host 127.0.0.1 --advertised-port 9092 --node-id 1 --data-dir data-1 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9093 --advertised-host 127.0.0.1 --advertised-port 9093 --node-id 2 --data-dir data-2 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9094 --advertised-host 127.0.0.1 --advertised-port 9094 --node-id 3 --data-dir data-3 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
```

Idempotent and transactional Kafka producers can use the cluster path. For meaningful durability, I use at least three voters, replication factor three, `min.insync.replicas=2`, and `acks=all`.

### Add or remove a voter online

I start a new node as an observer by pointing `--cluster-nodes` at the existing bootstrap voters while giving it a new `--node-id`, listener, and data directory. The local node does not need to appear in that bootstrap list. Then I use the standard Kafka 4.3 Admin API:

```java
Uuid directoryId = Uuid.randomUuid();
admin.addRaftVoter(
    4,
    directoryId,
    Set.of(new RaftVoterEndpoint("CONTROLLER", "127.0.0.1", 9095))
).all().get();
```

I keep that directory ID with the node identity. `describeMetadataQuorum()` also reports the committed voter identities and endpoints. Before removing a voter, I reassign every partition replica away from it; Cascade rejects removal while any assignment still references that broker. Then I call `removeRaftVoter(nodeId, directoryId)`. If that node is the active controller, Cascade commits the new voter set before stepping down.

## Kafka wire compatibility

Cascade returns exactly this matrix from `ApiVersions`:

| API | Key | Versions | Implemented behavior |
| --- | ---: | ---: | --- |
| Produce | 0 | 3 | `acks` 0/1/all, idempotent sequence validation/deduplication, transactional batches |
| Fetch | 1 | 6 | `read_uncommitted`/`read_committed`, high watermark, last stable offset, batch-aligned limits |
| ListOffsets | 2 | 2 | Isolation-aware earliest (`-2`) and latest (`-1`) offsets |
| Metadata | 3 | 4 | Broker/topic/partition discovery and optional auto-creation |
| OffsetCommit | 8 | 5-7 | Generation/static-member validation, leader epochs where present, and durable multi-partition commits |
| OffsetFetch | 9 | 4-5 | Requested or all committed group offsets with version-correct leader-epoch fields |
| FindCoordinator | 10 | 2 | Classic group and transaction coordinator discovery |
| JoinGroup | 11 | 5 | Classic membership, protocol selection, and leader election |
| Heartbeat | 12 | 3 | Session liveness and generation validation |
| LeaveGroup | 13 | 2 | Explicit departure and rebalance initiation |
| SyncGroup | 14 | 3 | Leader assignments and follower synchronization |
| SaslHandshake | 17 | 1 | Negotiate `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, or `OAUTHBEARER` before authentication |
| ApiVersions | 18 | 0-4 | Legacy and flexible encodings with tagged fields |
| CreateTopics | 19 | 2 | Validation and quorum-committed topic metadata |
| InitProducerId | 22 | 1 | Durable producer IDs, epoch allocation, and fencing |
| AddPartitionsToTxn | 24 | 1 | Transaction partition enrollment and timeout start |
| AddOffsetsToTxn | 25 | 1 | Consumer-group enrollment in a transaction |
| EndTxn | 26 | 1 | Durable commit/abort outcome and offset-application checkpoint |
| TxnOffsetCommit | 28 | 2 | Staged offsets made visible only by transaction commit |
| DescribeAcls | 29 | 1 | List exact, prefix, wildcard, allow, and deny ACL bindings |
| CreateAcls | 30 | 1 | Atomically persist and activate Kafka ACL bindings |
| DeleteAcls | 31 | 1 | Filter, remove, persist, and report matching ACL bindings |
| DescribeConfigs | 32 | 2 | Read-only non-sensitive broker and effective topic configuration for Kafka Admin |
| SaslAuthenticate | 36 | 1 | Kafka-framed PLAIN, multi-step SCRAM, or RFC 7628 OAUTHBEARER exchange and session lifetime |
| IncrementalAlterConfigs | 44 | 0 | Quorum-commit supported per-topic cleanup and retention changes |
| AlterPartitionReassignments | 45 | 0 | Start, replace, or cancel a durable online replica move |
| ListPartitionReassignments | 46 | 0 | Report intermediate, adding, and removing replicas |
| DescribeQuorum | 55 | 0-2 | Metadata leader, epoch, high watermark, voter identities, and endpoints |
| AddRaftVoter | 80 | 0-1 | Synchronize an observer and commit a durable joint-consensus admission |
| RemoveRaftVoter | 81 | 0 | Validate identity and partition drain, then remove and hand off leadership if needed |

I follow the [Apache Kafka 4.3 protocol grammar](https://kafka.apache.org/43/design/protocol/). Cascade only advertises and accepts the versions listed above; I don't want to claim support for versions I haven't tested.

## Architecture

```text
Kafka client in any language
        |
        | optional TLS + Kafka length-delimited TCP frames
        v
Virtual-thread connection handler
        |
        | connection cap + SASL identity + request quota/admission
        v
Request routing
        |
        +-- deny-by-default resource ACLs + durable JSONL audit
        +-- metadata quorum + durable joint membership, election, and fencing
        +-- classic group coordinator + durable offset journal
        +-- producer/transaction coordinator + delivery journal
        +-- partition log
              +-- opaque Kafka magic-v2 batches
              +-- broker-assigned offsets
              +-- bounded producer-state index
              +-- segment rollover and crash-tail recovery
              +-- background or synchronous flushing
              +-- ISR follower replication and committed watermark
```

I keep records serialized on the data path. Positional file I/O avoids shared channel-position races, each partition serializes offset assignment, and Fetch returns complete record batches. A transactional append reserves its range before storage. That stops `EndTxn` from racing an in-flight append and keeps the last stable offset behind open work.

## Heavy-load test details

### Ten-million sustained workload

```text
records             10,000,000
payload             1,024 bytes, deterministic incompressible variants
partitions          8
producer clients    4
consumer clients    4
compression         LZ4
acks                all
batch.size          128 KiB
linger.ms           5
segment size        256 MiB
flush policy        periodic
flush interval      1,000 ms
flush byte limit    64 MiB per partition
```

| Metric | Result |
| --- | ---: |
| Produce throughput | **182,285 records/s / 178.0 MiB/s** |
| Produce elapsed | **54.859 s** |
| Produce CPU | 1.75 cores |
| Ack latency p50 | <=1,000 ms |
| Ack latency p95 | >5,000 ms |
| Ack latency max | 19,017.787 ms |
| Force operations during produce | 131 |
| Data explicitly forced | 9,805.2 MiB |
| Cumulative force time | 47,624.5 ms |
| Consume throughput | **473,058 records/s** |
| Consume elapsed | **21.139 s** |
| Consumer verification | **10,000,000 / 10,000,000 passed** |
| Peak shared-JVM heap | 5,221.3 MiB |

I start the acknowledgement timer when each asynchronous `send` is offered, so saturation latency includes producer-side queueing. The producers offered data faster than my local temporary drive could force it during a sustained ten-gigabyte run. That's why p95 stayed above five seconds after the flush fix.

### One-million calibration

| Metric | Result |
| --- | ---: |
| Produce throughput | **614,413 records/s / 600.0 MiB/s** |
| Produce elapsed | 1.628 s |
| Ack latency max | 449.623 ms |
| Force operations during produce | 8 |
| Consume throughput | **556,232 records/s / 543.2 MiB/s** |
| Consumer verification | **1,000,000 / 1,000,000 passed** |
| Peak shared-JVM heap | 1,259.0 MiB |

After adding the cluster path, I ran this test again. It reached 615,592 produced and 527,001 consumed records/s, with p99 acknowledgement latency at or below 500 ms and a 422.632 ms maximum. I didn't find a meaningful single-node regression, but this short cache-assisted test isn't a sustained storage benchmark.

After adding dynamic membership and retry rollback, I ran the same one-million-record regression on 2026-08-17. It verified exactly **1,000,000 / 1,000,000** records at **367,202 produced records/s** and **250,135 consumed records/s**. The run used 7.43 producer-side CPU cores on my shared development machine, so I treat it as a correctness and regression signal rather than a new capacity claim.

After adding deterministic fault injection and forced-kill recovery qualification, I ran a clean **99/99** test suite and repeated the workload on 2026-08-20. It verified exactly **1,000,000 / 1,000,000** records at **503,499 produced records/s** (**491.7 MiB/s**) and **250,190 consumed records/s** (**244.3 MiB/s**). Produce took 1.986 seconds, consume took 3.997 seconds, p99 acknowledgement latency stayed at or below 500 ms, maximum acknowledgement latency was 445.075 ms, and peak heap was 1,222.1 MiB. I use this short cache-assisted run as a correctness and hot-path regression gate, not as a production capacity claim.

After adding storage lifecycle management, I ran a clean **125/125** test suite and repeated the workload on 2026-08-22. It verified exactly **1,000,000 / 1,000,000** records at **423,662 produced records/s** (**413.7 MiB/s**) and **477,368 consumed records/s** (**466.2 MiB/s**). Produce took 2.360 seconds, consume took 2.095 seconds, p99 acknowledgement latency stayed at or below 1,000 ms, maximum acknowledgement latency was 643.996 ms, and peak heap was 1,747.6 MiB. The lifecycle interval is five minutes by default, so no cleanup ran during this seven-second regression; I use it to detect hot-path and exactness regressions, not to claim lifecycle throughput.

After adding client-listener security and resource isolation, I ran a clean **149/149** suite, added two final rotation/tool checks, and repeated the one-million workload twice on 2026-08-24. Both runs verified exactly **1,000,000 / 1,000,000** records. The repeat produced **544,611 records/s** (**531.8 MiB/s**) and consumed **291,753 records/s** (**284.9 MiB/s**); the first run measured 545,911 and 295,007 records/s. Produce p99 stayed at or below 1,000 ms and the repeat's maximum was 537.726 ms. The plaintext load harness leaves authentication, ACLs, and quotas disabled, so I use this as a default-path regression gate rather than a security-capacity benchmark.

After adding the operations and offline-recovery milestone, I ran the full suite twice and finished with **180/180** passing tests. I repeated the one-million workload on 2026-08-24 and verified exactly **1,000,000 / 1,000,000** records at **533,937 produced records/s** (**521.4 MiB/s**) and **263,944 consumed records/s** (**257.8 MiB/s**). Produce took 1.873 seconds, consume took 3.789 seconds, p99 acknowledgement latency stayed at or below 500 ms, maximum acknowledgement latency was 415.569 ms, and peak heap was 1,395.1 MiB. The run forced 559.0 MiB in eight operations and left 427.2 MiB for clean shutdown. The operations listener was disabled in this harness, so I treat this as a default-path regression gate rather than monitoring or backup throughput.

After adding secure broker-to-broker transport, I fixed an interruption-close race found by the clean suite and finished with **200/200** passing tests. I then ran all **10,000,000** records on 2026-08-24 and consumed exactly **10,000,000 / 10,000,000** at **350,319 produced records/s** (**342.1 MiB/s**) and **329,303 consumed records/s** (**321.6 MiB/s**). Produce took 28.545 seconds, consume took 30.367 seconds, maximum acknowledgement latency was 3,636.325 ms, and peak heap was 5,336.7 MiB. The run stored 9,862.3 MiB and forced 9,674.0 MiB in 235 operations during production. Because this harness remains single-node plaintext, I use it as an exactness and inactive-security hot-path guard rather than a peer TLS capacity claim.

After adding Kafka-compatible SCRAM-SHA-256/512, strict bounded exchanges, offline verifiers, live atomic rotation, per-mechanism telemetry, and readiness integration, I finished with **221/221** passing tests. I reran all **10,000,000** records on 2026-08-27 and consumed exactly **10,000,000 / 10,000,000** at **333,835 produced records/s** (**326.0 MiB/s**) and **349,851 consumed records/s** (**341.7 MiB/s**). Produce took 29.955 seconds, consume took 28.584 seconds, maximum acknowledgement latency was 3,989.008 ms, and peak heap was 5,387.4 MiB. The run stored 9,862.3 MiB and forced 9,675.9 MiB in 247 operations during production. I use this single-node plaintext run as an exactness and inactive-authentication regression gate, not as authenticated or replicated capacity.

After adding signed OAuth/OIDC JWT authentication, strict JSON/JWKS parsing, verified HTTPS refresh with ETags, issuer/audience/time/scope policy, signing-key rotation, expiring connection identities, and operational integration, I finished with **244/244** passing tests. I reran all **10,000,000** records on 2026-08-30 and consumed exactly **10,000,000 / 10,000,000** at **368,387 produced records/s** (**359.8 MiB/s**) and **351,784 consumed records/s** (**343.5 MiB/s**). Produce took 27.145 seconds, consume took 28.427 seconds, maximum acknowledgement latency was 3,213.600 ms, and peak heap was 5,391.9 MiB. The run stored 9,862.3 MiB and forced 9,710.2 MiB in 221 operations during production. I use this single-node plaintext run as an exactness and inactive-OAuth regression gate, not as authenticated or replicated capacity.

After adding the production container path, I finished a clean **247/247** suite and built a 38.7 MB distroless image. I verified a non-root read-only standalone container, graceful named-volume recovery, the single-broker Compose deployment, and an external Kafka 4.3.1 client. I also brought up the three-broker Compose topology and produced and consumed 25/25 exact idempotent `acks=all` records with replication factor three and minimum ISR two. Both amd64 and arm64 release builds complete, and the 2026-08-30 Docker Scout scan found zero critical, high, medium, or low vulnerabilities among 15 detected packages. This container smoke is a deployment and interoperability gate; it does not replace the ten-million throughput qualification above.

After adding atomic TLS key/trust-store reload, last-known-good recovery, readiness/events/metrics, and generation-aware peer reconnection, I ran a clean **255/255** suite on 2026-08-31. Real Kafka clients kept established sessions through listener certificate and mutual-trust replacement, fresh clients proved the new material, and stale trust/client certificates were rejected. The RF=3 test moved from the old CA through overlapping trust and three live leaf rotations to the new CA, kept exact `acks=all` traffic moving, then removed the original controller and consumed all 25 committed records from the majority. I did not rerun the ten-million plaintext load harness for this milestone because the new work is active only on TLS connections; the published OAuth-milestone run remains my latest default-path measurement.

After the security, quota, per-topic policy, static-member, fuzz, deployment, and client-matrix work, I ran a clean **290/290** milestone suite on 2026-08-31. I also built the 1.0.0 distroless image, verified its non-root read-only runtime, crossed the container boundary with Java, JavaScript, Python, Go, and .NET clients, found no broker protocol errors, restarted it, and recovered the exact Java smoke records from the same volume. KafkaJS exposed an OffsetCommit v5/OffsetFetch v4 compatibility defect during this gate; the broker now supports both version shapes and the 1.0.0 protocol-contract test prevents an accidental API-range reduction.

After the rolling-feature, coordinator-sharding, consumer-heartbeat-v0, online-snapshot, advanced-compaction, distributed-quota, and qualification-harness work, I ran the complete suite again on 2026-08-31: **307/307 passed** in **2 minutes 8 seconds**. The gate includes Kafka 4.3.1 server-side consumer assignment, secure controller failover after live TLS rotation, and classic/transactional coordinator failover under sharding. Coordinator eligibility is now quorum committed, so clients stop routing shards to a broker after its failure is committed. I still treat the 72-hour soak and physical power/device-loss campaigns as unexecuted external release gates.

On 2026-09-01 I built the real 1.0.0 source at `c61264bf304719403b77c9b60709801be544373e` beside 1.1.0 and ran the three-broker rolling gate. It upgraded one broker, rolled it back before activation, upgraded all three brokers one at a time, activated the 1.1.0 feature map, rejected an unsafe 1.0.0 downgrade after activation, kept traffic moving on the majority, restored the new broker, and consumed exactly **40/40** ordered records. The campaign found an eager metadata-format rewrite that made pre-activation rollback unsafe; default persistence now stays on the minimum required format until a feature is committed.

I then reran the one-million default-path regression with 1 KiB LZ4 payloads, eight partitions, four producers, four consumers, `acks=all`, and periodic flushing. It consumed exactly **1,000,000 / 1,000,000** records at **396,006 produced records/s** (**386.7 MiB/s**) and **186,386 consumed records/s** (**182.0 MiB/s**). Produce took 2.525 seconds, consume took 5.365 seconds, maximum acknowledgement latency was 778.271 ms, and peak heap was 1,789.6 MiB. I use this single-node development-machine run as an exactness/hot-path regression, not as replicated production capacity.

### Reproduce the load test

```bash
./sbt "Test/runMain cascade.performance.LoadTest --records 10000000 --payload-bytes 1024 --partitions 8 --producers 4 --consumers 4 --compression lz4 --flush-policy periodic --flush-interval-ms 1000 --flush-bytes 67108864"
```

On Windows, replace `./sbt` with `.\sbt.bat`. Add `--keep-data` if you want to keep the generated segment directory. I leave this harness out of normal `sbt test` runs because it's large and its result depends on the machine.

The harness reports:

- Produce and consume records/s and payload MiB/s.
- Acknowledgement p50, p95, p99, p99.9, and maximum latency.
- Process CPU, machine CPU, GC collections, and GC time.
- Stored bytes, bytes per record, force count/volume/time, and pending dirty bytes.
- Peak heap and exact consumed-record verification.

The [complete 2026-08-05 report](docs/performance/2026-08-05-heavy-load.md) compares this result with the old per-request-force implementation and explains what changed.

## Verification

On 2026-09-02 I qualified shard-scoped coordinator commits with **3,000 offset writes across 1,000 groups**, eight concurrent Kafka clients, all three coordinator owners, controller failover, and full-cluster restart. Every final offset matched. Proposed delta payloads were **98.2% smaller** than equivalent full images, but the churn-heavy local run measured only **30.455 writes/s** with **4,011.691 ms p99** while approaching the host's dynamic-port limit. I do not present that as a production-capacity pass. The [full report](docs/performance/2026-09-02-coordinator-scale.md) records the workload, constraints, and remaining full-image quorum bottleneck. The same milestone passed the one-million-record regression exactly and reran all ten rolling-upgrade phases with exact 40/40 records.

I then qualified incremental coordinator journal/peer records at version 1.2.0: **3,000 writes and 1,000/1,000 exact offsets** passed through failover and restart, with measured delta traffic on disk and wire. Throughput was **25.664 writes/s** and p99 was **4,103.894 ms**, so this is still not production-capacity evidence. The latest one-million-record regression consumed exactly **1,000,000** records at **468,221 produced records/s** and **515,511 consumed records/s**. Both pinned rolling campaigns passed 40-record and committed-offset checks.

The current Scala test suite passes **414/414 tests**, covering unit, TCP integration, Kafka-client end-to-end, security, and fault/recovery layers. [Bounded offset batching](docs/offset-batching.md) adds admission limits, atomic validation/read isolation, lifecycle-safe publication, and a fix for OffsetFetch readiness races. Five 1,000-group campaigns each verified **3,000 writes and 1,000/1,000 final offsets** through controller loss and full restart. The latest million-record regression verified every record at **493,225 produced records/s** and **489,511 consumed records/s**. Linux storage/batching passed **51/51** tests, and all five external languages verified 25/25 records, with Java restart recovery. I archive the [current qualification and limitations](docs/performance/2026-09-03-offset-batching.md); the [earlier shard-storage profiling](docs/performance/2026-09-02-shard-storage.md) remains historical. Shared locks and full-state CPU work still prevent a production-capacity claim. Every advertised Metadata version (v4-v12) remains schema-tested, and anonymous idempotent producers bootstrap through every broker. The separate external-language matrix remains a CI gate:

- Unit tests for binary codecs, the frozen 1.0.0 API contract, record batches, storage/coordinator recovery, delivery semantics, cluster metadata, SCRAM, strict JSON/JWKS parsing, RSA/EC/Ed25519 JWT validation, role mapping, credential and peer policy, TLS reload/rejection, quotas, metrics, health/readiness, capacity evaluation, structured-log rotation, backup integrity, deployment artifacts, and maintenance commands.
- TCP integration tests for discovery, Produce/Fetch, idempotence, OffsetCommit v5-v7 and OffsetFetch v4-v5, flexible voter/config framing, TLS, PLAIN, both SCRAM mechanisms, OAUTHBEARER, malformed exchanges, peer impersonation rejection, live TLS/identity/credential/key/ACL rotation, auditing, directional quotas, and operational HTTP state.
- Kafka 4.3.1 end-to-end tests for Admin/Producer/Consumer interoperability, ACL administration, per-topic configuration, static-member fencing, SCRAM and OAUTHBEARER with `SASL_SSL`, listener and peer PKI rotation, encrypted RF=3 failover, consumer groups, reassignment, dynamic voters, transactions, coordinator failover, and exact restored data.
- Qualification tests that force-kill real broker JVMs, interrupt durable-store shutdown, corrupt persisted tails, partition stable and joint quorums, exercise retention and low-disk rejection, tamper with backup contents, and verify exact recovery, minority fencing, transition resumption, and dual-majority write safety.
- External process tests for KafkaJS, confluent-kafka Python, franz-go, and Confluent.Kafka .NET, each with exact 25/25 record validation and broker-side protocol-error rejection.

The load harness separately checks exact record counts at one million and ten million records. The rolling workflow builds the pinned 1.0.0 and current runtimes, rotates three real broker processes, and archives its JSON evidence. The container workflow builds the image, enforces its non-root metadata, crosses the Docker boundary with real clients, restarts the broker, and verifies exact recovery from the same named volume. CI also renders every Kubernetes resource and parses the Grafana dashboard.

## What I plan to build next

| Priority | Area | Planned work |
| ---: | --- | --- |
| 1 | Coordinator capacity | Independent per-shard consensus/publication beyond the shared atomic reference journal, finer-grained service locks, less full-state CPU work, membership/transaction churn at scale, and dedicated-host capacity qualification |
| 2 | Qualification | Run and archive the 72-hour multi-tenant soak, physical power/device-loss probe, restore drill, arbitrary packet impairment, and dedicated-host RF=3 benchmark |
| 3 | Consumer groups | Add administrative group APIs and continue expanding ConsumerGroupHeartbeat beyond v0 |
| 4 | Storage lifecycle | Snappy/LZ4/Zstd record rewriting and replicated retention coordination |
| 5 | Operations/security | A cross-node snapshot coordinator/manifest, scheduled retention, opaque-token introspection, OIDC discovery, and a built-in or documented external TLS boundary for operations |
| 6 | Profile-driven optimization | Zero-copy Fetch, selector/worker pools, multi-device log placement, and further changes justified by profiling |

I track the release gates in [docs/production-readiness.md](docs/production-readiness.md). I won't call Cascade a production Kafka replacement until every blocking gate passes on the deployment topology I document.

## What is still missing

- Dynamic membership, peer capability exchange, metadata-format negotiation, quorum-committed feature activation, and the pinned 1.0.0-to-1.1.0 rolling/rollback gate are implemented. Automatic broker registration, real OS power-loss testing, and exhaustive failure schedules during every joint phase are not complete.
- Replica recovery and reassignment transfer bounded record-batch chunks rather than zero-copy segment files; Produce is briefly fenced for the final delta and metadata transition.
- Coordinator ownership is rendezvous-sharded and changed shards have independent conflict versions. [Incremental replication](docs/incremental-coordinator.md) and [immutable shard-object persistence](docs/shard-storage.md) avoid full payload images for consecutive coordinator-only writes, with exact-base replay and snapshot fallback. Publication, in-memory state, and service locks remain shared. I still need independent per-shard consensus/journals and dedicated-host membership/transaction churn evidence before treating it like Kafka's partitioned internal topics. Windows object history is deliberately retained when directory forcing is unavailable; I do not claim bounded Windows disk usage.
- Compaction rewrites individual uncompressed/gzip records, preserves keyless records, applies tombstone grace, recalculates CRC32C, and supports an I/O ceiling. Snappy/LZ4/Zstd, control, and transactional batches remain opaque.
- Client authentication supports PLAIN, SCRAM-SHA-256/512, and signed OAUTHBEARER JWTs with RSA, EC, and Ed25519 keys plus approved claim-to-role mapping. I still need opaque-token introspection, automatic OIDC discovery, and revocation integration.
- I split each configured principal rate and burst conservatively across the current quorum, which bounds aggregate traffic without a central hot-path service. I still need long authenticated multi-tenant qualification and reclaiming unused shares without exceeding the cluster limit.
- The built-in operations listener is HTTP, so I still require an external TLS/mTLS boundary for non-loopback deployments. Online snapshots now stop admitted writes, force every local partition, and pass exact restore tests. A full cluster backup still needs coordinated per-host artifacts, scheduled retention, encrypted off-host transfer, and repeated restore drills.
- I support classic groups and `ConsumerGroupHeartbeat` v0 with broker-side assignment; later protocol versions and the administrative group APIs remain.
- The automated client matrix covers one pinned release each of Java, JavaScript, Python, Go, and .NET, and the 1.0.0-to-1.1.0 broker matrix passes; broader client versions and every future adjacent broker-version pair remain.
- The performance figures are single-node, shared-JVM development-machine measurements; replicated-cluster capacity has not been benchmarked.
- The forced-kill suite validates process loss and torn tails, and the two-phase physical probe records every acknowledged offset on an independent witness device. The probe is implemented, but I will not claim power/device-loss qualification until I cut real host/device power and the post-restart verifier passes on the target hardware.

## Configuration

| Option | Default | Meaning |
| --- | --- | --- |
| `--host` | `0.0.0.0` | Listener bind host |
| `--port` | `9092` | Listener port; `0` selects a free test port |
| `--advertised-host` | `localhost` | Host returned by Metadata |
| `--advertised-port` | Listener port | Port returned by Metadata |
| `--data-dir` | `data` | Topic segments and internal journal root |
| `--max-request-bytes` | `104857600` | Hard request-frame bound |
| `--segment-bytes` | `134217728` | Segment rollover target |
| `--flush-policy` | `periodic` | Batched background forcing or strict per-append `sync` |
| `--flush-interval-ms` | `1000` | Maximum periodic dirty age |
| `--flush-bytes` | `67108864` | Per-partition dirty-byte threshold that schedules a force |
| `--cleanup-policy` | `delete` | Default `delete`, `compact`, or combined `delete,compact` lifecycle policy; topic overrides are quorum committed |
| `--retention-ms` | `604800000` | Default age limit for closed committed segments; `-1` disables time retention |
| `--retention-bytes` | `-1` | Default per-partition byte budget; `-1` disables size retention |
| `--delete-retention-ms` | `86400000` | Time to retain the newest tombstone before compaction can erase the key history |
| `--compaction-max-bytes-per-second` | `-1` | Per-partition compaction rewrite limit; `-1` disables throttling |
| `--lifecycle-interval-ms` | `300000` | Interval between lifecycle maintenance passes |
| `--minimum-free-bytes` | `0` | Free-space reserve below which Cascade rejects new appends before writing |
| `--offset-retention-ms` | `604800000` | Age limit for committed consumer offsets; `-1` disables expiry |
| `--journal-compaction-bytes` | `67108864` | Local offset, delivery, and cluster-metadata journal compaction threshold |
| `--node-id` | `1` | Broker/controller ID |
| `--cluster-nodes` | Empty | Initial voters or observer discovery endpoints as `id@host:port`; empty selects single-node mode |
| `--controller-id` | `1` | Preferred initial controller candidate; any configured voter can be elected later |
| `--default-replication-factor` | `1` | Replication factor used for auto-created topics |
| `--min-insync-replicas` | `1` | Minimum ISR required by `acks=all` |
| `--peer-timeout-ms` | `3000` | Internal metadata and replica RPC timeout |
| `--replica-recovery-timeout-ms` | `300000` | Maximum controller wait for one replica recovery operation |
| `--replica-recovery-chunk-bytes` | `8388608` | Maximum record-batch payload requested per incremental recovery transfer |
| `--controller-heartbeat-ms` | `250` | Elected-controller heartbeat interval |
| `--controller-election-timeout-ms` | `1500` | Controller lease and minimum election timeout; must be at least three heartbeat intervals |
| `--offset-batch-max-requests` | `64` | Maximum clustered OffsetCommit commands per publication batch |
| `--offset-batch-max-bytes` | `1048576` | Maximum estimated retained command bytes in one batch |
| `--offset-batch-pending-requests` | `1024` | Maximum queued plus in-flight offset commands |
| `--offset-batch-pending-bytes` | `16777216` | Maximum estimated queued plus in-flight command bytes |
| `--offset-batch-linger-ms` | `2` | Maximum intentional accumulation delay |
| `--offset-batch-queue-timeout-ms` | `5000` | Maximum wait before staging, not a publication deadline |
| `--security-protocol` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` client listener |
| `--ssl-keystore` | Empty | PKCS12 or JKS server key store; required by `SSL` and `SASL_SSL` |
| `--ssl-keystore-password-file` | Empty | UTF-8 file containing the key-store password |
| `--ssl-key-password-file` | Key-store password | Optional separate private-key password file |
| `--ssl-truststore` | JVM default | PKCS12 or JKS client trust store; required when client certificates are requested |
| `--ssl-truststore-password-file` | Empty | UTF-8 file containing the trust-store password |
| `--ssl-client-auth` | `none` | `none`, `requested`, or `required` TLS client-certificate verification |
| `--tls-protocols` | `TLSv1.3,TLSv1.2` | Enabled TLS protocol list |
| `--ssl-reload-ms` | `1000` | Key/trust-store fingerprint interval; zero disables live reload and a bad replacement preserves the last valid context |
| `--peer-security-protocol` | `PLAINTEXT` | `SSL` enables hostname-verified mTLS for internal RPCs and requires an SSL listener, trust store, client-certificate verification, and identity file |
| `--peer-identity-file` | Empty | Node-ID-to-X.500-subject policy required by peer `SSL` |
| `--peer-identity-reload-ms` | `1000` | Interval for atomic peer identity policy reload; the last valid policy survives a malformed replacement |
| `--credentials-file` | Empty | PBKDF2 credential file required when `PLAIN` is enabled |
| `--scram-credentials-file` | Empty | Offline SCRAM verifier file required when either SCRAM mechanism is enabled |
| `--sasl-mechanisms` | `PLAIN` | Comma-separated `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, and/or `OAUTHBEARER` mechanisms advertised by the listener |
| `--oauth-jwks-uri` | Empty | Required absolute `https:` or `file:` JWKS URI when `OAUTHBEARER` is enabled |
| `--oauth-issuer` | Empty | Required exact JWT issuer |
| `--oauth-audience` | Empty | Required audience that must occur in JWT `aud` |
| `--oauth-principal-claim` | `sub` | JWT string claim used as the ACL principal |
| `--oauth-scope-claim` | `scope` | JWT string or string-array claim containing scopes |
| `--oauth-role-claim` | Empty | Optional JWT string or string-array claim whose approved values become local roles |
| `--oauth-role-map` | Empty | Comma-separated `claim-value=local-role` allowlist; required with a role claim |
| `--oauth-required-scopes` | Empty | Comma-separated scopes every token must contain |
| `--oauth-allowed-algorithms` | `RS256` | Comma-separated allowlist of `RS256/384/512`, `ES256/384/512`, and `EdDSA` |
| `--oauth-clock-skew-seconds` | `30` | JWT time-claim allowance from 0 through 300 seconds |
| `--oauth-jwks-refresh-ms` | `300000` | Background JWKS refresh interval; zero refreshes on access |
| `--oauth-http-timeout-ms` | `5000` | HTTPS JWKS connection/request timeout from 100 through 60000 ms |
| `--oauth-max-token-bytes` | `16384` | OAUTHBEARER JWT bound from 1 KiB through 1 MiB |
| `--credential-reload-ms` | `1000` | Interval for atomic credential snapshot reload |
| `--sasl-session-lifetime-ms` | `0` | Session lifetime reported by `SaslAuthenticate`; zero disables reauthentication expiry |
| `--acl-file` | Empty | Deny-by-default resource ACL file; no file leaves authorization disabled |
| `--acl-reload-ms` | `1000` | Interval for atomic ACL snapshot reload |
| `--super-users` | Empty | Comma-separated principals that bypass ACL evaluation |
| `--audit-log` | Empty | Append-only JSONL destination for authentication and authorization events |
| `--audit-buffered` | Off | Skip per-event `force(false)`; shutdown still forces and closes the audit log |
| `--max-connections` | `10000` | Global active connection cap |
| `--max-connections-per-ip` | `1000` | Active connection cap for one source IP |
| `--max-inflight-requests` | `10000` | Global request permits before overload shedding closes a connection |
| `--request-bytes-per-second` | `0` | Per-principal ingress quota; zero disables quota work |
| `--request-burst-bytes` | Quota rate | Per-principal token-bucket burst size |
| `--response-bytes-per-second` | `0` | Per-principal egress quota; zero disables it |
| `--response-burst-bytes` | Quota rate | Per-principal egress burst size |
| `--produce-bytes-per-second` | `0` | Additional per-principal Produce ingress quota |
| `--produce-burst-bytes` | Quota rate | Produce-specific burst size |
| `--fetch-bytes-per-second` | `0` | Additional per-principal Fetch egress quota |
| `--fetch-burst-bytes` | Quota rate | Fetch-specific burst size |
| `--max-throttle-ms` | `1000` | Maximum quota delay; larger required delays shed the request connection |
| `--operations-host` | `127.0.0.1` | Separate HTTP operations bind host |
| `--operations-port` | Empty | Enable the operations listener; `0` selects a free test port |
| `--operations-token-file` | Empty | UTF-8 bearer token file; required for a non-loopback operations host and at least 32 characters |
| `--structured-log` | Empty | Rotating JSONL operational event destination |
| `--structured-log-max-bytes` | `67108864` | Rotate the operational event file before the next event would exceed this size |
| `--structured-log-retained-files` | `5` | Number of rotated operational event generations |
| `--no-stderr-log` | Off | Disable structured operational events on standard error |
| `--readiness-max-pending-flush-bytes` | `9223372036854775807` | Pending dirty-byte ceiling for readiness |
| `--capacity-alert-interval-ms` | `30000` | Capacity evaluation interval |
| `--capacity-connection-ratio` | `0.85` | Active-connection utilization that raises an alert |
| `--capacity-inflight-ratio` | `0.85` | In-flight request utilization that raises an alert |
| `--capacity-pending-flush-bytes` | `536870912` | Pending dirty-byte capacity alert; zero disables this check |
| `--capacity-minimum-free-bytes` | `0` | Usable-disk alert and readiness reserve; zero disables this extra reserve |
| `--capacity-alert-repeat-ms` | `300000` | Repeat interval for an alert that remains active |
| `--no-auto-create` | Off | Disable Metadata/Produce auto-creation |

## More documentation

- [Production-readiness gates](docs/production-readiness.md)
- [External Kafka client compatibility matrix](compatibility/README.md)
- [Container deployment runbook](docs/containers.md)
- [Rolling upgrade and downgrade runbook](docs/rolling-upgrades.md)
- [Coordinator scaling and qualification](docs/coordinator-scaling.md)
- [TLS key and trust rotation runbook](docs/tls-rotation.md)
- [OAuth and OIDC authentication runbook](docs/oauth-oidc.md)
- [SCRAM authentication runbook](docs/scram-authentication.md)
- [Broker-to-broker security runbook](docs/peer-security.md)
- [Operations runbook](docs/operations.md)
- [Backup and restore runbook](docs/backup-restore.md)
- [Soak and physical-loss qualification](docs/qualification.md)
- [Heavy-load report](docs/performance/2026-08-05-heavy-load.md)
- [Contributing](CONTRIBUTING.md)
- [Apache-2.0 license](LICENSE)

## License

Apache License 2.0.

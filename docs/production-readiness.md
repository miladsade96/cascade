# Production-readiness checklist

I want Cascade to become a real Kafka replacement, but it isn't there yet. This is the checklist I'm using to keep that goal honest and measurable.

## What is working now

- Kafka TCP framing with a small, explicit API and version matrix.
- Topic creation, metadata, produce, fetch, and offset lookup in single-node and dynamically reconfigurable cluster modes.
- Kafka magic-v2 batches stored without decoding or recompressing them.
- Size-based segments, restart discovery, partial-tail recovery, and synchronous or batched periodic flushing.
- Classic consumer-group join, sync, heartbeat, leave, rebalance, and session expiry.
- A CRC32C-protected committed-offset journal that is forced to disk and recovered after restart.
- Idempotent producer IDs, epoch fencing, bounded duplicate detection, sequence validation and recovery, transactions, timeouts, transactional offsets, and `read_committed` isolation in single-node and clustered modes.
- Metadata images committed by stable or dual joint majorities, durable voter endpoints and directory IDs, controller terms and votes, quorum election, controller leases, broker fencing, round-robin replica assignment, leader epochs, ISR failure detection, and promotion of a surviving replica.
- Parallel synchronous leader-to-follower replication, `min.insync.replicas` checks, committed high watermarks, and leader-only fetch and offset lookup.
- Checksum-protected, double-buffered partition high-watermark checkpoints that recover conservatively after torn or corrupt writes.
- Incremental replica recovery that verifies a chained batch fingerprint, truncates only the divergent suffix, transfers bounded committed chunks while Produce continues through the existing ISR, briefly fences the final delta, and re-admits the replica after a quorum metadata commit.
- Durable online partition reassignment with learners outside the ISR, add-before-remove ordering, atomic target finalization, replacement plans, cancellation, and controller-failover resume.
- Kafka-compatible `AlterPartitionReassignments` v0 and `ListPartitionReassignments` v0 Admin APIs.
- Kafka-compatible `DescribeQuorum` v0-v2, `AddRaftVoter` v0-v1, and `RemoveRaftVoter` v0 Admin APIs.
- Observer synchronization, joint-consensus admission/removal, interrupted-transition resume, partition-drain validation, and active-controller handoff.
- A versioned quorum image for classic-group membership and assignments, committed offsets, producer fencing, active transaction ranges, outcomes, and transactional offsets.
- Stale coordinator-image rejection, rollback on quorum loss, controller-only expiry, and atomic transaction/outcome offset checkpoints.
- Deterministic directional peer partitions and protocol-triggered message drops without using timing as the fault trigger.
- Clean/unclean startup markers plus real subprocess force-kill tests that bypass broker shutdown hooks.
- Exact data, transaction, producer-epoch, and committed-offset recovery after force kill, including conservative truncation of torn data, offset, and delivery journal tails.
- Stable-quorum majority service, minority coordinator fencing, durable joint-transition resume, controller loss during joint consensus, and dual-majority write rejection tests.
- Scheduled time/size retention that retires only closed committed segments and preserves log-end offset continuity across broker restart.
- Atomic `.deleted` and `.cleaned` rename protocols with startup completion for interrupted segment deletion, compaction, and coordinator-journal replacement.
- Conservative whole-batch key compaction for uncompressed non-transactional records, with opaque retention of compressed, keyless, control, and transactional batches.
- Rebuilt batch timestamp and transaction-range indexes, durable offset expiry, and bounded offset, delivery, and cluster-metadata journals.
- Pre-append free-space admission that leaves the log end unchanged and returns Kafka `KAFKA_STORAGE_ERROR` to clients.
- TLS 1.2/1.3 listeners with PKCS12/JKS keys, optional client-certificate verification, and Kafka-compatible authentication using PLAIN, SCRAM-SHA-256/512, or OAUTHBEARER; bearer tokens require `SASL_SSL`.
- Hostname-verified mutual TLS for controller, metadata, replication, and recovery RPCs, with a distinct certificate identity bound to every claimed node ID.
- Atomic peer-identity policy reload that preserves the last valid snapshot, supports overlapping rotation identities, fails readiness on malformed replacements, and audits every accepted or denied internal request.
- Offline salted SCRAM verifiers, salted PBKDF2-SHA-256 PLAIN credentials, and signed OAuth/OIDC JWTs validated against bounded RSA JWKS, exact issuer/audience/time/scope policy, constant-time password verification, bounded exchanges, expiring connection principals, mechanism-aware audit/metrics, and atomic credential/key rotation that preserves the last valid snapshot and gates readiness on malformed replacements.
- Deny-by-default topic, group, transaction, and cluster ACLs with exact/prefix/wildcard matching, explicit-deny precedence, super users, and live atomic policy rotation.
- Forced JSONL authentication/authorization audit events with escaped fields, source address, TLS state, principal, decision, operation, and resource.
- Global and per-IP connection caps, a bounded in-flight request semaphore, immediate overload shedding, and isolated per-principal request-byte token buckets.
- A separate loopback-default operations listener with bearer authentication for remote binds, liveness/readiness/status JSON, and bounded-label Prometheus 0.0.4 metrics.
- Readiness gates for broker state, fencing, flush backlog, disk reserve, structured-log health, peer identity policy, and credential policy.
- Size-rotated JSONL broker events plus deduplicated connection, request, flush-backlog, and disk-capacity alert/resolution events.
- Kafka-compatible `DescribeConfigs` v2 for non-sensitive, read-only broker and effective topic configuration, verified through Kafka Admin 4.3.1.
- Offline backup creation with an exact SHA-256 manifest, source stability checks, forced copies, and atomic publication; verification and restore reject tampering, extra files, traversal, symlinks, and existing targets.
- A Kafka-client disaster-recovery test that produces records, shuts down, backs up, restores into a new data directory, starts in clean-recovery mode, and consumes the exact values.
- A three-broker Kafka-client security test that replicates over peer mTLS, stops the original broker/controller, resumes `acks=all` production on the majority, and consumes the exact committed sequence.
- A 38.7 MB distroless JDK 21 container that runs as UID/GID 65532, uses a module-limited Java runtime and internal readiness probe, supports read-only-root operation and persistent volumes, and has single/three-broker Compose qualification plus multi-architecture Docker Hub release automation with SBOM and provenance.
- Real Kafka 4.3.1 client tests for Admin, Producer, explicit Consumer, subscribed consumers, rebalancing, committed-offset recovery, broker restart, three/four-node replication, partition-leader shutdown, controller/coordinator loss, stale-term rejection, post-election metadata changes, reassignment cancellation, reassignment through controller loss, voter admission, active-controller removal/restart, and an open transaction completed after failover.
- Real Kafka 4.3.1 `SASL_SSL` Admin/Producer/classic-group Consumer tests for SCRAM-SHA-256/512 and OAUTHBEARER that cross authentication, verifier/JWKS rotation, wrong-token denial, ACL, storage, offset commit, TLS trust, metrics, readiness, and audit paths.
- Exact-count load tests at one million and ten million records with latency, CPU, GC, heap, storage, and flush metrics.

## What I still need before a production release

| Area | What must pass before release | Where Cascade is now |
| --- | --- | --- |
| Availability | At least three brokers, replicated partitions, ISR tracking, leader election, and verified recovery from broker, process, and disk loss | Graceful and forced-kill recovery, persisted high-watermarks, replica re-admission, stable/joint partition tests, and online reassignment pass; physical power and device-loss qualification remains |
| Metadata | Durable quorum controller, fencing, leader epochs, reassignment, and cluster membership changes | Durable election, leases, fencing, metadata images, leader epochs, resumable reassignment, joint consensus, deterministic partition recovery, and dual-majority write safety work; rolling-version and exhaustive external failure qualification remain |
| Delivery semantics | Idempotent producers, sequence validation, transactions, producer-state recovery, and `read_committed` isolation | Implemented in single-node and clustered modes; Kafka-client failover plus forced-kill/torn-tail transaction, producer, and offset recovery tests pass, while coordinator scale tests remain |
| Storage lifecycle | Time and size retention, log and offset compaction, timestamp and transaction indexes, disk-pressure handling, and safe deletion | Broker-wide retention, conservative uncompressed whole-batch key compaction, rebuilt indexes, offset expiry, bounded coordinator journals, atomic replacement recovery, and low-disk rejection pass; per-topic policy, compressed-record compaction, tombstone grace, deletion throttling, and external disk-failure qualification remain |
| Security | TLS, SASL mechanisms, authorization and ACLs, audit events, and secret rotation | Client TLS, hostname-verified peer mTLS, certificate-bound nodes, Kafka PLAIN, SCRAM-SHA-256/512, and signed OAuth/OIDC OAUTHBEARER, offline verifiers, HTTPS/file JWKS refresh, resource ACLs, forced JSONL audit, and live identity/credential/key/ACL policy rotation pass; opaque-token introspection, automatic discovery, TLS key-store hot reload, Kafka ACL Admin APIs, claim-to-role policy, and multi-tenant qualification remain |
| Resource isolation | Client and user quotas, request and connection limits, bounded queues, overload shedding, and multi-tenant tests | Global/per-IP connection caps, bounded in-flight requests, per-principal ingress token buckets, request frame bounds, and immediate shedding pass; response/egress and distributed quotas, protocol throttle fields, and long multi-tenant soak tests remain |
| Operations | Metrics, health and readiness endpoints, structured logs, admin API coverage, backup and restore, container deployment, and capacity alerts | Broker metrics/health/status, rotating events, capacity signals, `DescribeConfigs` v2, offline checksummed backup/restore, a non-root distroless image, hardened Compose examples, multi-architecture publishing automation, runbooks, and exact Kafka-client recovery tests pass; external TLS/mTLS, standard dashboards/Alertmanager integration, orchestrator manifests, online cluster snapshots, scheduled retention, and recurring restore drills remain |
| Compatibility | Supported-version contract, malformed-frame and fuzz tests, multiple client languages, and a rolling upgrade and downgrade matrix | Java-client coverage is partial |
| Consumer groups | New consumer protocol, static-member fencing, administrative group APIs, offset retention and compaction, and coordinator failover | Classic group generations, members, assignments, and offsets survive elected-coordinator loss; offset expiry and atomic journal compaction work, while the new protocol, static fencing, admin APIs, and scale qualification remain |
| Qualification | Multi-day soak tests, crash, kill, and power-failure simulation, network partitions, disk-full and corruption tests, and repeatable dedicated-host benchmarks | Deterministic stable/joint partitions, protocol-message drops, subprocess force kills, and torn-tail recovery pass; physical power loss, disk loss/full, arbitrary network impairment, long soak, and dedicated-host benchmarks remain |

## When I will call it production ready

I won't describe a release as a Kafka replacement until every blocking row above has an automated acceptance test and passes on the deployment topology I document.

When I publish a performance result, I'll include the hardware, durability policy, workload, client configuration, and exact delivery count. When I claim compatibility, I'll list the API keys and versions instead of just saying "Kafka compatible."

I completed the OAuth/OIDC authentication milestone and the production container path: RFC 7628 framing, signed RSA JWT verification, strict bounded JSON/JWKS parsing, exact issuer/audience/time/scope policy, verified HTTPS refresh with ETags, last-valid-key rotation, readiness/audit/metrics integration, expiring connection identity, and real Kafka client coverage over TLS now ship in a non-root distroless image with persistent-volume and Compose qualification. The full suite passes **247/247** tests, the container smoke passes exact idempotent Kafka traffic before and after restart, the three-broker Compose smoke passes replication factor three with minimum ISR two, and my ten-million-record default-path regression consumed exactly **10,000,000 / 10,000,000** records. My next security work is TLS key/trust-store hot reload, Kafka ACL Admin APIs, claim-to-role policy, egress/distributed quotas, and multi-tenant soak qualification.

# Production-readiness gates

Cascade is not yet a production replacement for an Apache Kafka cluster. This document makes that boundary testable instead of treating "production ready" as a marketing label.

## Implemented foundation

- Kafka TCP framing and a deliberately narrow, advertised API/version matrix.
- Topic creation, metadata, produce, fetch, and offset lookup in single-node or static-cluster mode.
- Kafka magic-v2 batches persisted without decoding or recompression.
- Size-based segments, restart discovery, partial-tail recovery, and configurable synchronous or batched periodic flushing.
- Classic consumer-group join, sync, heartbeat, leave, rebalance, and session expiry.
- CRC32C-protected, forced committed-offset journal with restart recovery.
- Idempotent producer IDs, epoch fencing, bounded duplicate detection, sequence validation/recovery, transactions, timeouts, transactional offsets, and `read_committed` isolation in single-node mode.
- Static-cluster metadata images durably accepted by a majority, controller quorum recovery, round-robin replica assignment, leader epochs, ISR failure detection, and surviving-replica promotion.
- Synchronous parallel leader-to-follower batch replication, `min.insync.replicas` admission, committed high-watermark visibility, and leader-only fetch/offset lookup.
- Real Kafka 4.3.1 client coverage for Admin, Producer, explicit Consumer, subscribed consumers, rebalance, committed-offset recovery, broker restart, three-node replication, and partition-leader shutdown.
- Exact-count one-million and ten-million record load tests with latency, CPU, GC, heap, storage, and flush metrics.

## Blocking gates for a Kafka replacement

| Area | Required release gate | Current state |
| --- | --- | --- |
| Availability | At least three brokers, replicated partitions, ISR tracking, leader election, and verified recovery from broker/process/disk loss | Static three-node replication and graceful leader-loss failover pass; crash/disk-loss recovery, catch-up and re-admission remain |
| Metadata | Durable quorum controller, fencing, leader epochs, reassignment, and cluster membership changes | Majority-replicated metadata images and partition leader epochs implemented; controller election, dynamic membership and reassignment remain |
| Delivery semantics | Idempotent producers, sequence validation, transactions, producer-state recovery, and `read_committed` isolation | Implemented and acceptance-tested in single-node mode; coordinator-state replication and failover remain blocking for cluster deployment |
| Storage lifecycle | Time/size retention, log and offset compaction, timestamp/transaction indexes, disk-pressure handling, and safe deletion | Not implemented |
| Security | TLS, SASL mechanisms, authorization/ACLs, audit events, and secret rotation | Not implemented |
| Resource isolation | Client/user quotas, request and connection limits, bounded queues, overload shedding, and multi-tenant tests | Partial frame bounds only |
| Operations | Metrics, health/readiness endpoints, structured logs, admin API coverage, backup/restore, and capacity alerts | Not implemented |
| Compatibility | Supported-version contract, malformed-frame/fuzz suite, multiple client languages, and rolling upgrade/downgrade matrix | Partial Java-client coverage |
| Consumer groups | New consumer protocol, static-member fencing, administrative group APIs, offset retention/compaction, and coordinator failover | Classic groups are pinned to the fixed controller; coordinator state is not replicated |
| Qualification | Multi-day soak, crash/kill/power-failure simulation, network partitions, disk-full/corruption tests, and reproducible dedicated-host benchmarks | Short correctness/load runs only |

## Acceptance policy

A release must not be described as a Kafka replacement until every blocking row above has an automated acceptance test and passes on the documented deployment topology. Performance claims must state hardware, durability policy, workload shape, client configuration, and exact delivery verification. Compatibility claims must list API keys and versions rather than say only "Kafka compatible."

The next highest-value engineering milestone is controller election plus offline-replica reconciliation and safe ISR re-admission. Until those are complete, losing the fixed controller stops metadata changes/group coordination and a returning replica cannot safely rejoin automatically.

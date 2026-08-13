# Production-readiness checklist

I want Cascade to become a real Kafka replacement, but it isn't there yet. This is the checklist I'm using to keep that goal honest and measurable.

## What is working now

- Kafka TCP framing with a small, explicit API and version matrix.
- Topic creation, metadata, produce, fetch, and offset lookup in single-node and static-cluster modes.
- Kafka magic-v2 batches stored without decoding or recompressing them.
- Size-based segments, restart discovery, partial-tail recovery, and synchronous or batched periodic flushing.
- Classic consumer-group join, sync, heartbeat, leave, rebalance, and session expiry.
- A CRC32C-protected committed-offset journal that is forced to disk and recovered after restart.
- Idempotent producer IDs, epoch fencing, bounded duplicate detection, sequence validation and recovery, transactions, timeouts, transactional offsets, and `read_committed` isolation in single-node mode.
- Static-cluster metadata images committed by a majority, durable controller terms and votes, quorum election, controller leases, broker fencing, round-robin replica assignment, leader epochs, ISR failure detection, and promotion of a surviving replica.
- Parallel synchronous leader-to-follower replication, `min.insync.replicas` checks, committed high watermarks, and leader-only fetch and offset lookup.
- Checksum-protected, double-buffered partition high-watermark checkpoints that recover conservatively after torn or corrupt writes.
- Incremental returning-replica recovery that verifies a chained batch fingerprint, truncates only the divergent suffix, transfers bounded committed chunks, fences Produce during the transition, and re-admits the replica after a quorum metadata commit.
- Real Kafka 4.3.1 client tests for Admin, Producer, explicit Consumer, subscribed consumers, rebalancing, committed-offset recovery, broker restart, three-node replication, partition-leader shutdown, controller loss, stale-term rejection, and post-election metadata changes.
- Exact-count load tests at one million and ten million records with latency, CPU, GC, heap, storage, and flush metrics.

## What I still need before a production release

| Area | What must pass before release | Where Cascade is now |
| --- | --- | --- |
| Availability | At least three brokers, replicated partitions, ISR tracking, leader election, and verified recovery from broker, process, and disk loss | Graceful partition-leader and controller failover, persisted high-watermark recovery, and incremental returning-replica re-admission pass; crash/power-loss qualification and reassignment remain |
| Metadata | Durable quorum controller, fencing, leader epochs, reassignment, and cluster membership changes | Durable majority election, controller leases, broker fencing, metadata images, and partition leader epochs work; dynamic membership and reassignment remain |
| Delivery semantics | Idempotent producers, sequence validation, transactions, producer-state recovery, and `read_committed` isolation | Implemented and acceptance-tested in single-node mode; coordinator-state replication and failover still block cluster deployment |
| Storage lifecycle | Time and size retention, log and offset compaction, timestamp and transaction indexes, disk-pressure handling, and safe deletion | Not implemented |
| Security | TLS, SASL mechanisms, authorization and ACLs, audit events, and secret rotation | Not implemented |
| Resource isolation | Client and user quotas, request and connection limits, bounded queues, overload shedding, and multi-tenant tests | Only frame bounds are implemented |
| Operations | Metrics, health and readiness endpoints, structured logs, admin API coverage, backup and restore, and capacity alerts | Not implemented |
| Compatibility | Supported-version contract, malformed-frame and fuzz tests, multiple client languages, and a rolling upgrade and downgrade matrix | Java-client coverage is partial |
| Consumer groups | New consumer protocol, static-member fencing, administrative group APIs, offset retention and compaction, and coordinator failover | Classic groups are routed to the elected controller, but coordinator state is not replicated |
| Qualification | Multi-day soak tests, crash, kill, and power-failure simulation, network partitions, disk-full and corruption tests, and repeatable dedicated-host benchmarks | Short correctness/load runs and a controlled replica restart/rejoin test are complete |

## When I will call it production ready

I won't describe a release as a Kafka replacement until every blocking row above has an automated acceptance test and passes on the deployment topology I document.

When I publish a performance result, I'll include the hardware, durability policy, workload, client configuration, and exact delivery count. When I claim compatibility, I'll list the API keys and versions instead of just saying "Kafka compatible."

My next availability milestone is partition reassignment and dynamic quorum membership. I also need to replicate group, offset, producer, and transaction coordinator state before controller failover can preserve those services.

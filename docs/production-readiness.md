# Production-readiness checklist

I want Cascade to become a real Kafka replacement, but it isn't there yet. This is the checklist I'm using to keep that goal honest and measurable.

## What is working now

- Kafka TCP framing with a small, explicit API and version matrix.
- Topic creation, metadata, produce, fetch, and offset lookup in single-node and dynamically reconfigurable cluster modes.
- Kafka magic-v2 batches stored without decoding or recompressing them.
- Size-based segments, restart discovery, partial-tail recovery, and synchronous or batched periodic flushing.
- Classic consumer-group join, sync, heartbeat, leave, rebalance, and session expiry.
- A CRC32C-protected committed-offset journal that is forced to disk and recovered after restart.
- Idempotent producer IDs, epoch fencing, bounded duplicate detection, sequence validation and recovery, transactions, timeouts, transactional offsets, and `read_committed` isolation in single-node mode.
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
- Real Kafka 4.3.1 client tests for Admin, Producer, explicit Consumer, subscribed consumers, rebalancing, committed-offset recovery, broker restart, three/four-node replication, partition-leader shutdown, controller/coordinator loss, stale-term rejection, post-election metadata changes, reassignment cancellation, reassignment through controller loss, voter admission, active-controller removal/restart, and an open transaction completed after failover.
- Exact-count load tests at one million and ten million records with latency, CPU, GC, heap, storage, and flush metrics.

## What I still need before a production release

| Area | What must pass before release | Where Cascade is now |
| --- | --- | --- |
| Availability | At least three brokers, replicated partitions, ISR tracking, leader election, and verified recovery from broker, process, and disk loss | Graceful and forced-kill recovery, persisted high-watermarks, replica re-admission, stable/joint partition tests, and online reassignment pass; physical power and device-loss qualification remains |
| Metadata | Durable quorum controller, fencing, leader epochs, reassignment, and cluster membership changes | Durable election, leases, fencing, metadata images, leader epochs, resumable reassignment, joint consensus, deterministic partition recovery, and dual-majority write safety work; rolling-version and exhaustive external failure qualification remain |
| Delivery semantics | Idempotent producers, sequence validation, transactions, producer-state recovery, and `read_committed` isolation | Implemented in single-node and clustered modes; Kafka-client failover plus forced-kill/torn-tail transaction, producer, and offset recovery tests pass, while coordinator scale tests remain |
| Storage lifecycle | Time and size retention, log and offset compaction, timestamp and transaction indexes, disk-pressure handling, and safe deletion | Not implemented |
| Security | TLS, SASL mechanisms, authorization and ACLs, audit events, and secret rotation | Not implemented |
| Resource isolation | Client and user quotas, request and connection limits, bounded queues, overload shedding, and multi-tenant tests | Only frame bounds are implemented |
| Operations | Metrics, health and readiness endpoints, structured logs, admin API coverage, backup and restore, and capacity alerts | Not implemented |
| Compatibility | Supported-version contract, malformed-frame and fuzz tests, multiple client languages, and a rolling upgrade and downgrade matrix | Java-client coverage is partial |
| Consumer groups | New consumer protocol, static-member fencing, administrative group APIs, offset retention and compaction, and coordinator failover | Classic group generations, members, assignments, and offsets survive elected-coordinator loss; the new protocol, static fencing, admin APIs, retention, compaction, and scale qualification remain |
| Qualification | Multi-day soak tests, crash, kill, and power-failure simulation, network partitions, disk-full and corruption tests, and repeatable dedicated-host benchmarks | Deterministic stable/joint partitions, protocol-message drops, subprocess force kills, and torn-tail recovery pass; physical power loss, disk loss/full, arbitrary network impairment, long soak, and dedicated-host benchmarks remain |

## When I will call it production ready

I won't describe a release as a Kafka replacement until every blocking row above has an automated acceptance test and passes on the deployment topology I document.

When I publish a performance result, I'll include the hardware, durability policy, workload, client configuration, and exact delivery count. When I claim compatibility, I'll list the API keys and versions instead of just saying "Kafka compatible."

I completed the deterministic process-kill, torn-tail, coordinator-write, and stable/joint network-partition milestone. My next milestone is storage lifecycle: time/size retention, log and coordinator compaction, offset expiry, indexes, disk-pressure handling, and crash-safe deletion. After that I need a sharded coordinator design so the current full-image path does not become a high-cardinality bottleneck.

# Coordinator publication batching

I use this milestone to reduce one measured coordinator bottleneck without pretending that Cascade now has independent coordinator Raft groups.

## Contract

Every coordinator owner still prepares a versioned replacement for only the virtual shards it changes. At the controller, I place those proposals in one bounded FIFO queue. After a short linger, the worker validates proposals in arrival order against a candidate metadata image:

- proposals for disjoint shards can enter one quorum publication;
- a multi-shard transaction is accepted or rejected as one unit;
- a stale term, stale shard version, malformed payload, or allocator rollback rejects only that proposal;
- later compatible proposals in the same batch remain eligible;
- all accepted proposals receive success only if the combined metadata image reaches quorum; and
- a failed quorum proposal returns an error to every proposal it would have committed.

The queue retains an estimated encoded byte count and enforces request and byte limits both per batch and across queued plus in-flight work. Oversized proposals fail before admission. A queued request may time out and be removed, but once publication begins the caller waits for the real quorum result so a timeout cannot trigger an unsafe local rollback. Shutdown rejects queued work and waits for an active publication before closing metadata storage.

This changes neither the peer RPC nor metadata format, so mixed-version compatibility remains controlled by the existing `coordinator-deltas` feature level. An old controller publishes proposals individually; a new controller may combine proposals from old or new owners.

## Configuration

| Option | Default | Meaning |
| --- | ---: | --- |
| `--coordinator-publication-max-requests` | `64` | Maximum proposals in one controller batch |
| `--coordinator-publication-max-bytes` | `16777216` | Maximum encoded bytes in one batch |
| `--coordinator-publication-pending-requests` | `1024` | Maximum queued plus in-flight proposals |
| `--coordinator-publication-pending-bytes` | `67108864` | Maximum estimated queued plus in-flight bytes |
| `--coordinator-publication-linger-ms` | `2` | Maximum wait used to collect compatible proposals |
| `--coordinator-publication-queue-timeout-ms` | `5000` | Deadline while a proposal is still cancellable in the queue |

I export pending/peak request and byte gauges plus admission, completion, failure, batch, committed, conflict, and queue-time counters. They have only `node_id`; group IDs, transactional IDs, and shard IDs are deliberately absent.

## Qualification

The unit tests cover request/byte admission, exact metrics, compatible coalescing, queue expiry, active-publication ambiguity, shutdown draining, stale conflicts, malformed payload isolation, multi-shard atomicity, and allocator monotonicity. A three-broker Kafka-client test selects twelve groups across distinct virtual shards and all three owners, commits concurrently, verifies fewer quorum publications than acknowledged requests, restarts every broker from disk, and reads every exact offset.

The scale runner records publication bounds and outcomes alongside existing snapshot, journal, replication, offset-batch, latency, failover, and restart evidence:

```text
./sbt "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --batch-max-requests 64 --batch-linger-ms 2 --publication-max-requests 64 --publication-linger-ms 2 --report artifacts/coordinator-publication.json"
```

## Remaining boundary

This removes redundant quorum rounds when compatible proposals arrive together. It does not remove the controller's metadata mutation lock, shared metadata quorum, full coordinator image held by every broker, group/delivery service lock, or per-proposal snapshot capture. Workloads that touch the allocator or the same hash bucket still conflict by design. Independent per-shard consensus and execution, finer-grained service locking, controlled dedicated-host RF=3 capacity, membership rebalance churn, and high-cardinality transaction churn remain release gates.

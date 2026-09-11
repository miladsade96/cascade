# Coordinator publication batching

This document describes the controller-publication path retained for metadata formats 9–11. The later [format-12 coordinator architecture](coordinator-architecture.md) moves steady-state coordinator mutations to forced per-shard quorum journals.

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

## Read isolation while publication waits

Offset and `read_committed` Fetch decisions no longer wait on the publication monitor. They capture immutable views derived only from the last acknowledged coordinator image. A staged proposal cannot enter those views, and a failed proposal leaves the existing objects unchanged. One Kafka Fetch response holds one transaction view across last-stable-offset and batch-visibility checks.

I gate this contract with a three-broker fault test that pauses one owner's commit RPC at the controller rather than sleeping for a guessed interval. `OffsetFetch` must finish with the previous acknowledged value while the write is paused, then expose the new value after the quorum result. The full design and metrics are in [acknowledged coordinator read isolation](coordinator-read-isolation.md).

## Remaining boundary

For formats 9–11 this removes redundant quorum rounds when compatible proposals arrive together, and acknowledged read views keep covered reads off the publication monitor. Format 12 bypasses this queue and the metadata quorum for steady-state coordinator mutations. Every broker still holds a complete coordinator image, group/delivery mutation remains locally shared, and workloads touching the allocator or the same hash bucket conflict by design. Distributed decided-transaction resolution, shard-journal compaction, finer-grained mutation locking, controlled dedicated-host RF=3 capacity, membership rebalance churn, and high-cardinality transaction churn remain release gates.

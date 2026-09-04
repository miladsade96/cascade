# Coordinator snapshot preparation

I use content-addressed, fixed-layout encoding caches to reduce the CPU and allocation cost of preparing coordinator checkpoints. This is an internal optimization in 1.3.0-SNAPSHOT, not a new wire protocol, metadata format, release image, or independent shard consensus implementation.

## Publication contract

I still capture group and delivery state under the same service lock. Transaction outcomes and committed consumer offsets remain one atomic proposal. After publication succeeds or fails, I install the latest authoritative quorum image; a rejected candidate never becomes the rollback baseline.

The delta-capable path now partitions immutable images directly. Each of the 64 group buckets, 64 transaction buckets, and producer allocator retains only its current input and encoded payload. Unchanged contents reuse immutable byte vectors. Equality checks the complete normalized contents, not a hash, global version, or shard version. This matters for failed proposals that reuse versions and provisional transaction ranges that change before their image version advances. A failed encode cannot partially publish that cache's replacement.

Legacy peers still use the existing full-image publication path until coordinator deltas are activated. Encoded shard bytes, checksums, metadata formats, and feature levels are unchanged. Decoded authoritative images are lazy, image-local caches; copying metadata with different bytes creates independent views.

I no longer serialize the installed services a second time just to reconstruct their baseline. The baseline comes from the committed image itself. The equivalent-full-image metric is computed from exact format headers and shard body lengths, including group formats 1 and 2; it does not require allocating the full candidate.

## Membership installation and expiry

Ordinary metadata installation is not a heartbeat. Existing member timestamps retain the newer local or replicated heartbeat instead of resetting to installation time. Initial recovery and controller-term changes grant recovery grace. Routing acquisition grants a session window once; temporary image-readiness lag does not grant repeated grace.

Each ready assigned group coordinator now expires its own classic and new-protocol members and retained offsets. The controller must not expire an active group whose heartbeats arrive at another broker. Transaction timeout processing remains controller-owned.

Classic group identities survive image reconciliation so blocked join/sync requests can see installed assignments. Deleting an authoritative group clears and wakes its existing waiters; it cannot leave a detached live group behind. The public recovery snapshot API continues granting a fresh session window.

Offset snapshots reuse a canonical immutable view until mutation. Identical authoritative offsets do not rebuild the store. Per-group offset lookup uses a group-key index, maintained during commit, recovery, replacement, and expiry.

## Metrics

These four counters add only a `node_id` label; there are now 103 fixed series per broker. They count local candidate preparation, including candidates later rejected by the quorum:

| Metric | Meaning |
| --- | --- |
| `cascade_coordinator_snapshot_encoded_shards_total` | Payloads re-encoded locally |
| `cascade_coordinator_snapshot_reused_shards_total` | Payloads reused locally |
| `cascade_coordinator_snapshot_encoded_bytes_total` | Bytes allocated as encoded candidate payloads; not total JVM allocation or disk traffic |
| `cascade_coordinator_snapshot_preparation_seconds_total` | Time capturing live state and preparing its shard candidate |

I compare rates of reused versus encoded shards, preparation time versus checkpoint time, and checkpoint failures alongside client latency. A high reuse ratio alone is not a capacity pass. Counter snapshots are observations, not an atomic accounting transaction. Legacy full-image preparation does not increment these four delta-path counters.

## Reproduction

I run the full Scala suite, then the preparation comparison and real-client campaigns:

```text
./sbt test
./sbt "Test/runMain cascade.qualification.CoordinatorSnapshotQualification --groups 1000 --iterations 500 --report artifacts/coordinator-snapshot.json"
./sbt "Test/runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 8 --rounds 2 --client-lifecycle persistent --report artifacts/coordinator-8.json"
./sbt "Test/runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --report artifacts/coordinator-32.json"
```

The preparation comparison verifies every candidate byte-for-byte before timing. Four trials alternate old/full and cached preparation order, with up to 100 excluded warmups per trial. It reports current-thread CPU and allocated bytes when supported (`-1` means unavailable). Inputs are prebuilt immutable images: the microbenchmark excludes live group capture, quorum, storage, and installation, and cannot be quoted as broker throughput.

The real-client runner includes snapshot counters in its JSON alongside exact offsets, failover/restart checks, batching, and physical metadata I/O. Its metric totals include warmup and retried requests. I retain raw failures as well as successful reports.

On Windows I use `scripts/qualify-shard-storage-linux.ps1` after compilation for the Linux storage, cache, membership, rollback, and snapshot-barrier checks. CI verifies correctness and actual reuse, with no hardware-specific speed threshold.

## Remaining limits

This is still one metadata quorum, one combined service lock, and whole-state traversal during live group capture. Controller merge, replication validation, storage checkpoints, and some installation work still encode or traverse complete images. The caches and offset index also retain additional heap proportional to current state; fixed shard count does not impose a fixed byte footprint.

I still need independent publication/consensus, finer-grained locking, high-cardinality membership and transaction churn campaigns, dedicated-host RF=3 measurements, and the external release qualification gates in [production readiness](production-readiness.md). Short automated session tests are not a multi-day membership soak.

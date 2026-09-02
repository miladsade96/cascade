# Incremental coordinator persistence and replication

I am removing complete coordinator images from the steady-state journal and peer commit paths. This is the next coordinator-capacity step, not independent per-shard consensus: publication, in-memory images, and service locks remain shared.

## Safety and acceptance contract

- A delta applies only to the exact committed metadata version, controller term, coordinator version, SHA-256 base fingerprint, and touched shard versions from which it was built. Numeric versions alone cannot distinguish divergent failed-quorum histories.
- A delta changes coordinator state only. Membership, topics, feature activation, controller transitions, and recovery use complete snapshots.
- The existing stable/joint quorum rules apply to both encodings. An out-of-date follower rejects a delta and receives a full snapshot.
- One forced CRC32C journal frame covers every shard in an atomic transaction. Replay must reject a checksummed delta with a missing/wrong base; it may discard an incomplete or checksum-corrupt tail under the existing recovery contract.
- Periodic atomic full checkpoints bound replay and retain the existing backup/restore contract.
- A new committed feature and storage-format floor gate activation. Mixed-version clusters retain their previous encoding until every voter supports the new format.
- I require codec/validation tests, torn-tail and checkpoint recovery, wire-level fault tests, Kafka offset/transaction recovery, the complete suite, and measured journal/replication bytes before claiming the optimization works.

Physical power loss, independent shard consensus, fine-grained service locks, multi-day load, and dedicated-host capacity remain separate release gates.

## Encoding and recovery

`incremental-coordinator` level 1 raises the complete-image compatibility floor to format 10. It requires `coordinator-deltas` and unanimous voter support. The journal continues using length/payload/CRC32C frames; a payload beginning with signed short `-10` contains the exact metadata/coordinator base versions and the existing atomic shard delta. Older binaries reject the format instead of interpreting incremental records as full images.

I use deltas only for consecutive coordinator-only changes in the same controller term. Feature activation, topology changes, elections, and non-consecutive recovery retain full snapshots. A follower acknowledges only after forcing its journal frame. Repeated delivery of its last delta is idempotent; a missing base returns an explicit rejection and the controller retries with a complete snapshot under the same quorum rules. A lost connection is not treated as a base rejection or a successful acknowledgement.

Compaction atomically replaces the journal with one complete image. The replay-byte budget excludes that checkpoint's own size, so an image larger than the configured compaction threshold does not cause every small delta to rewrite the entire checkpoint. Checksummed but invalid deltas fail startup without truncating the evidence. Incomplete/CRC-corrupt tails retain the previous recovery behavior.

## Measurements

I expose node-only metrics for forced full/delta record counts and bytes, checkpoint bytes, current journal size, attempted full/delta replication bytes, and base-rejection fallbacks. Journal counters include framing and count successful forced writes; replication counters include RPC bodies, retries, and full snapshot responses, but exclude Kafka framing, TCP, and TLS overhead. Counters reset when a broker restarts.

```text
./sbt "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 8 --rounds 2 --report artifacts/incremental-coordinator-scale.json"
```

The report aggregates each broker's counters before that broker stops. Its byte totals cover startup, writes, verification, and controller failover, but not the final full-cluster restart. Its latency/throughput still includes per-group client creation and closure. Verifier client churn can exhaust a small host port range, so this remains a shared-host correctness campaign. A byte reduction is not evidence of independent consensus or production throughput.

## Upgrade gate

```powershell
./scripts/qualify-rolling-upgrade.ps1 -Baseline format9 -Report artifacts/incremental-rolling-format9.json
./scripts/qualify-rolling-upgrade.ps1 -Baseline 1.0.0 -Report artifacts/incremental-rolling-1.0.0.json
```

The format-9 baseline is the real source at `8c8ff4cc63955d23eb566683bc1ac473e767bae3`, whose version string was 1.1.0. It is **not** the published format-8 Docker Hub image. The gate preserves the baseline feature map through partial upgrade and rollback, activates the new map only after replacing all voters, rejects an unsafe downgrade, and verifies `acks=all` traffic plus exact committed consumer offsets. Historical binaries restrict anonymous producer initialization to their controller, so rolling traffic disables idempotence; current-binary idempotence is tested separately. Published-release qualification against the format-8 image remains separate.

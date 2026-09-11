# Independent coordinator qualification — 2026-09-11

I qualified the format-12 coordinator architecture at broker/test revision `d6f1d0a8b15d6594e00fd589509f34f0cefd9f26`. The tracked documentation commit follows that revision and does not change broker or test code.

## What I implemented

- Typed group and transaction keys with separate 64-shard namespaces and one producer allocator shard.
- The `independent-coordinator` feature level and metadata format 12, activated only after unanimous voter support.
- CRC32C-framed per-shard journals with bounded records, forced appends, incomplete-tail truncation, and corruption rejection.
- Atomic multi-shard prepare, majority decision, finalize, abort, restart resume, and monotonic image merging.
- A bounded fair admission gate plus sorted shard locks, allowing disjoint quorum-layer transactions to progress concurrently without deadlocking overlapping transactions.
- Internal prepare/decide/finalize/abort peer APIs, follower installation, controller-term fencing, and steady-state routing outside the metadata-controller publication log.
- Node-scoped quorum, phase, byte, pending-transaction, journal-size, force-time, and torn-tail metrics.
- Backup/restore coverage for the coordinator-quorum directory and exact recovery after controller loss and complete broker restart.

## Test evidence

| Gate | Result |
| --- | --- |
| Focused coordinator, cluster, read-isolation, recovery, end-to-end, and metrics matrix | **81/81 passed** |
| Legacy format-11 publication regression after explicit capability pinning | **8/8 passed** |
| Complete Scala unit, integration, end-to-end, fault, security, and qualification suite | **525/525 passed** in 152 seconds |
| Persistent capacity campaign | **1,000/1,000** exact offsets after controller failover and full restart |
| Shard quorum | **672/672** committed; zero failure or admission rejection |
| Offset batching | 4,000 requests in 672 batches; zero failure or rejection |
| Journal recovery | 28,908 records; 63,881,354 bytes; zero torn-tail truncation |

The first complete-suite attempt exposed seven legacy publication assertions that had silently moved onto format 12 after feature activation. I added an explicit format-11 capability fixture for those tests. Their isolated rerun passed 8/8, followed by the clean 525/525 complete run. The initial Java 11 launcher attempt was invalid because Cascade requires Java 21; all reported evidence uses Eclipse Adoptium 21.0.11.

## Capacity result

I kept 1,000 Kafka consumers resident, used 32 workers, performed one warm-up write and three measured writes per group, stopped the controller, verified through failover, restarted every broker from its existing directory, and verified every final offset.

| Measurement | Result |
| --- | ---: |
| Measured writes | 3,000 |
| Write time / throughput | 8.136 s / **368.719 writes/s** |
| Commit latency p50 / p95 / p99 | 81.623 / 114.494 / **256.974 ms** |
| Coordinator owners used | 1, 2, and 3 |
| Acknowledged offset snapshots / returned keys | 3,001 / 3,000 |
| Encoded / equivalent full-image bytes | 3,704,555 / 40,105,122 |
| Quorum phase messages | 2,016 prepare / 1,895 decide / 1,895 finalize |
| Encoded quorum record bytes | 11,232,043 |
| Aggregate journal force time | 9.693 s |

The exact machine-readable result is in the [JSON report](2026-09-11-independent-coordinator.json).

## Result and remaining boundary

The architecture milestone passes its local correctness, durability, failover, recovery, bounded-admission, observability, and regression gates. Format-12 coordinator mutations no longer depend on a metadata-controller log append, and the older format-11 path remains independently qualified for mixed-version operation.

This is not yet Kafka-equivalent coordinator capacity. The end-to-end campaign reached a quorum peak of one because group and delivery mutation still shares one broker-local service lock. A different owner cannot yet resolve a transaction whose original coordinator dies after its majority decision but before terminal finalization. Per-shard journals do not yet checkpoint or enforce a tested indefinite-churn disk budget. I also have not run the format-11-to-format-12 rolling campaign, dedicated Linux RF=3 capacity test, high-cardinality membership and transaction churn, or a multi-day soak.

I therefore mark the independent quorum foundation complete and keep coordinator recovery, compaction, service-lock decomposition, rolling qualification, and dedicated-host capacity as release blockers.

# Coordinator snapshot qualification — 2026-09-04

I completed the snapshot preparation and membership-installation increment in **1.3.0-SNAPSHOT**. The last broker-code change is `b7eb4c130ce919934d961617280aec9e2f8c49e3`. The expanded full test run started at `a431a067e0c056899efdb95abdbd7539e7370050`; subsequent commits archive evidence and update documentation. Individual workload reports retain their observed source revision.

The staged application JAR SHA-256 is `19bcb89b00ba046756f83480660c9a870b25dfd5463d6eddb1d27552dc3debb6`. All three rolling reports identify that same artifact. I did not change the release version, build or publish a Cascade image, or push Git commits.

## Changes and correctness

I replaced full candidate encode/decode with immutable typed partitioning and content-validated caches for 64 group buckets, 64 transaction buckets, and the allocator. Unchanged payloads reuse their byte vectors; same-version changes, rollback, deletions, heartbeat updates, and hash collisions cannot return stale encodings. Authoritative metadata shares decoded views, and installation uses its committed shard baseline instead of reserializing both services. Canonical offset snapshots and a group-key index reduce repeated sorting and full-store group reads.

I also found and fixed installation behavior that treated every metadata update as a fresh heartbeat. Ordinary installation now preserves actual member liveness. Recovery and ownership acquisition grant grace; readiness lag alone does not repeatedly renew it. Ready group owners perform expiry because they receive their members' heartbeats. Transaction expiry remains controller-owned. Classic group objects survive assignment installation, and authoritative deletion wakes blocked sync calls with an unknown member rather than leaving detached live state.

The [runbook](../coordinator-snapshots.md) covers atomic publication, rollback, bounded cache retention, metrics, mixed-version behavior, and remaining limits. No protocol or storage-format change is required.

## Preparation comparison

The [paired report](2026-09-04-snapshot-preparation.json) uses 1,000 group offsets, 1,000 producer registrations, and 500 prebuilt mixed updates. Every candidate is checked byte-for-byte against the previous full encode/decode/split path before measurement. Four trials alternate measurement order and exclude 100 warmup candidates per trial.

| Path | Median time for 500 candidates | Mean current-thread allocated bytes | Shards encoded / reused per trial |
| --- | ---: | ---: | ---: |
| Full serialization | 441.397 ms | 1,419,118,624 | 64,500 / 0 |
| Cached preparation | 124.214 ms | 348,393,396 | 696 / 63,804 |

That is **3.55× lower median preparation time** and **75.45% less measured allocation**. Current-thread CPU is also retained in the JSON. This comparison excludes live mutable-group capture, quorum RPCs, storage, and installation. It is not broker throughput, a heap-retention bound, or a dedicated-host capacity result.

## Real-client coordinator capacity

Each campaign uses three in-process brokers with separate temporary directories on the same SSD, RF=3, min ISR=2, synchronous storage, and 1,000 persistent Kafka 4.3.1 clients. The runner excludes 1,000 warmup writes from latency/throughput, then performs 3,000 timed writes. All three owners participate; exact offsets are checked before and after controller loss and a complete broker restart. Client timeouts are 5 seconds per request and 30 seconds per API call. The fault fixture uses 300 ms peer timeouts, 100 ms heartbeats, and 600 ms elections, not deployment tuning recommendations.

Batching remains 64 requests / 1 MiB with 2 ms linger; admission remains 1,024 requests / 16 MiB. The persistent connection budget is 3,032 per IP. Both runs report **zero batch and connection admission rejections**.

| Workers | Writes/s | Write seconds | p50 ms | p95 ms | p99 ms | Final offsets |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| [8](2026-09-04-snapshot-persistent-8.json) | 41.249 | 72.729 | 70.551 | 601.158 | 2,117.376 | 1,000/1,000 |
| [32](2026-09-04-snapshot-persistent-32.json) | 97.773 | 30.683 | 128.413 | 1,160.486 | 2,303.257 | 1,000/1,000 |

| Workers | Dispatched requests / batches | Admitted request errors | Checkpoint attempts / failures | Encoded / reused shards | Preparation time, cumulative |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | 4,626 / 3,735 | 626 | 3,248 / 33 | 10,030 / 408,962 | 2.713 s |
| 32 | 5,388 / 1,929 | 1,388 | 1,737 / 224 | 12,138 / 211,935 | 1.678 s |

Counters include warmup and retries, whereas timed latency excludes warmup and includes client retries. Early handler errors are outside batching counters. Peak retained commands/estimated bytes were 7/4,354 and 23/14,198. At 32 workers, about **25.8%** of dispatched requests still return errors before retries succeed. Zero admission rejection does not mean zero retry pressure. The runs preserve correctness but **do not pass a production latency/capacity SLO**.

The prior milestone measured 24.480 and 41.922 writes/s at 8/32 workers. Those are historical observations, not same-process or same-day controls. The new campaigns are single runs on a shared development machine; lightweight Dockerfile/actionlint validation overlapped part of the 32-worker campaign. I do not attribute every end-to-end difference to caching or claim statistical confidence. Only the preparation comparison above alternates matched implementations in the same process. The previous large churn and ten-million-record campaigns remain historical; I did not rerun them in this increment.

## Whole-software qualification

| Check | Result |
| --- | --- |
| Full Scala unit/integration/E2E/security/fault suite | **445/445**, 158 seconds; earlier full pass was 442/442 before three additional view/metric tests |
| Linux storage/coordinator/cache/session suite | **79/79**, 16.327 seconds |
| Linux directory-force probe | Supported; 134 objects / 8,452 bytes reclaimed, 97 bytes retained |
| One-million default-path load | **1,000,000 / 1,000,000** exact records |
| Java 4.3.1, KafkaJS 2.2.4, Python confluent-kafka 2.15.0, franz-go 1.21.0, .NET Confluent.Kafka 2.15.0 | **25/25 each**, plus Java restart recovery of the same 25 |
| Dockerfile validation | `docker build --check .` passed, no warnings; previous distroless registry denial no longer reproduced |
| Kubernetes / dashboard / workflows / scripts | 1,095 rendered lines; dashboard JSON, Actionlint 1.7.7, and PowerShell parsing passed |
| Final deployment/release-document consistency check | **5/5** tests; new documentation links and all six archived JSON reports validated |

The million-record workload is single-node plaintext with 1 KiB payloads, eight partitions, four producers/four consumers, LZ4, and periodic flushing at 1 second / 64 MiB. It produced **468,358 records/s** and consumed **463,448 records/s**; acknowledgement p99 was in the **≤500 ms** histogram bucket and maximum acknowledgement latency was **431.037 ms**. Peak sampled heap was **934.5 MiB**. Produce used 8.87 CPU cores with 24 GC collections / 88 ms; consume used 5.63 cores with 35 collections / 62 ms. The produce-end flush sample recorded 622.2 MiB forced and 364.0 MiB pending. This is not a synchronous-durability or RF=3/TLS throughput claim.

External clients ran against the staged runtime in a restricted Linux JDK container, not a newly built Cascade release image. Go used direct upstream downloads with TLS and pinned checksums intact. KafkaJS emitted the existing local Node timer warning but its exact check passed. The script removed its uniquely named containers; synthetic data remains under ignored artifacts for inspection. Linux tmpfs checks exercise filesystem semantics, not physical power-loss durability.

## Rolling compatibility

All three pinned source pairs passed ten phases: live replacement, pre-activation rollback, complete upgrade, feature activation, unsafe-downgrade rejection, recovery, and exact 40-record consumption with offset progress checks:

| Baseline | Elapsed traffic campaign | Evidence |
| --- | ---: | --- |
| Format 10 / source 1.2.0 | 10.493 s | [Report](2026-09-04-snapshot-rolling-format10.json) |
| Format 9 / source 1.1.0 | 10.086 s | [Report](2026-09-04-snapshot-rolling-format9.json) |
| Source 1.0.0 | 14.198 s | [Report](2026-09-04-snapshot-rolling-1.0.0.json) |

These use non-idempotent rolling traffic to accommodate the old anonymous-producer restriction. Current idempotency/transactions are covered separately by the full suite. The format-9 source pair is not the published format-8 image pair. Temporary historical worktrees were cleaned up. No rollout or publishing operation was performed.

## Environment, raw evidence, and remaining gates

The host is an Intel Core i5-12450HX with 12 logical processors and 21.72 GiB OS-visible RAM, running Java 21.0.11 and Scala 3.3.8. Benchmark temporary directories are on C:, a 512.1 GB WD PC SN5000S SDEPMSJ-512G-1101 SSD using NTFS; the project lives on D:. All brokers share the host and device. Windows does not provide the directory-force guarantee required for automatic shard-object reclamation, so retained history is expected there.

Raw local logs are `artifacts/snapshot-full-qualification.log`, `snapshot-final-regression.log`, `snapshot-linux.log`, `snapshot-external-clients.log`, `snapshot-docker-check.log`, `snapshot-actionlint.log`, and `snapshot-rolling-{format10,format9,1.0.0}.log`. Controlled storage/quorum/authentication fault tests intentionally emit error logs; the test runners report no failed tests. Archived JSON reports above preserve the measured values rather than replacing them with estimates.

I still need independent shard publication/consensus, finer-grained service locking, remaining whole-state CPU reductions, high-cardinality transaction/membership churn, and dedicated-host RF=3 capacity evidence. Multi-day authenticated soak, physical power/device loss, arbitrary packet impairment, coordinated multi-host restore drills, published-image upgrade qualification, broader protocol/admin coverage, and the other [production-readiness gates](../production-readiness.md) are still open. This increment improves performance and correctness; it does not make Cascade a production-grade Kafka replacement.

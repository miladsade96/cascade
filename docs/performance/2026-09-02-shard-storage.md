# Shard-object coordinator qualification — 2026-09-02

I qualified development source `a43e8a8786fcb2ec6743f148aacd705002ab1b32`, version **1.3.0-SNAPSHOT**. The final evidence/documentation commit does not change the broker. This is a coordinator-storage increment, not a production release or a newly published Docker image.

## Scope

Changed coordinator shard payloads now live in immutable, namespace-bound SHA-256 objects. The existing forced metadata journal publishes compact references atomically across every touched shard. Format 11 and unanimous feature negotiation gate activation. Migration and structural checkpoints preserve byte-exact full images; peer replication retains the existing inline delta/snapshot protocol.

Recovery rejects missing or corrupt referenced objects without trimming a valid commit marker. Orphan preparations remain unpublished. Reclamation requires a self-contained checkpoint and durable directory publication. A failed journal/checkpoint operation fences metadata service until restart, including ambiguous post-publication failures. Backup and restore include both the journal and object directory.

I also added bounded, warmed, persistent Kafka clients; actual object I/O/admission counters; immutable image-local shard caches; and a Linux directory-forcing/reclamation probe. The [storage runbook](../shard-storage.md) documents the contract and commands. Independent per-shard consensus, service-lock removal, and removal of all full-state CPU work are **not** implemented by this increment.

## Findings during qualification

- Repeated tests exposed a real follower-first publication race. Heartbeats could observe a follower's durable candidate before its leader finished publishing locally and incorrectly demote that leader. Reconciliation now waits for the metadata mutation to settle; higher terms still fence immediately. A reply-barrier regression failed before the fix and passes afterward. Failed-quorum, stale-term, and partition regressions remain in the full suite.
- A real broker checkpoint failure now verifies service fencing after an ambiguous durable publication, rather than testing the store alone.
- The initial 1,000-client runs timed out during failover offset lookup. The single-IP harness retained bootstrap, metadata, and coordinator connections against a 1,000-connections-per-IP budget. Persistent mode now explicitly provisions `max(1000, 3 * groups + 32)` and requires zero admission rejections, including after restart. Production defaults, peer timers, and the 30-second client API timeout are unchanged. The final run completes the previously failing phase and exact recovery.
- Development and deployment versions are explicit: source/build metadata is 1.3.0-SNAPSHOT; `deploy/VERSION` and deployment examples retain the previously tested local 1.2.0 image. Release tests permit this difference only for snapshot development.

## Acceptance evidence

The final complete unit/integration/Kafka-Java/security/fault suite passed **388/388** tests in **143 seconds**, and `stage` built the dependency-complete runtime. The earlier full run passed 387/387 before adding the persistent connection-budget regression. Focused heartbeat, recovery-bound, and release-alignment checks also passed.

The separate Linux run passed **24/24** storage, backup, heartbeat-overlap, and broker-fencing tests. Its strict probe reported `directory_force=true`, **134 objects / 8,452 bytes reclaimed**, **97 bytes retained**, and exact restart recovery. This ran on Linux `/tmp` (tmpfs) in a disposable pinned Temurin JDK 21 container. It exercises the directory-forcing/reclamation code path, not physical device durability.

Dockerfile checking completed without warnings, Kubernetes rendering produced 1,076 lines, and the Grafana dashboard parsed successfully.

The external matrix passed **25/25 records each** with Java Kafka 4.3.1, KafkaJS 2.2.4, Python confluent-kafka 2.15.0, franz-go 1.21.0, and Confluent.Kafka .NET 2.15.0. Java recovered the same 25 records after broker restart, and the broker log contained no protocol errors. I ran the staged 1.3.0-SNAPSHOT jars inside a pinned Temurin JDK 21 container as UID/GID 65532, with a read-only root, dropped capabilities, no-new-privileges, a writable test data mount, and a 2 GiB memory limit. This was **not** a new distroless image build or Docker Hub publication. The disposable broker was removed; logs and synthetic data remain in local `artifacts/`.

The first .NET attempt was blocked by a non-executable temporary build mount; permitting execution on that disposable SDK build mount resolved it. KafkaJS on local Node 26.3.0 emitted a negative-timer warning but completed exact verification; I retain the warning in the raw log instead of treating this small smoke as broad Node-version qualification.

## Persistent-client measurements

The [machine-readable report](2026-09-02-shard-persistent.json) records **3,000 timed offset writes across 1,000 groups**, eight workers, all three coordinator owners, and **1,000/1,000 exact final offsets** after controller failover and full-cluster restart. The same 1,000 clients stayed open throughout. A separate 1,000-write warmup took 39.571 seconds.

Environment: Windows/NTFS, Java 21.0.11, Intel Core i5-12450HX (8 cores/12 logical processors), shared development host and SSD. Three in-process brokers used independent directories on that SSD, RF=3, minimum ISR=2, and synchronous metadata durability. Fixture peer/heartbeat/election timers remained 300/100/600 ms; these are not production recommendations. Java Flight Recorder used `settings=profile`, `dumponexit=true`, and a 128 MiB maximum recording.

| Measurement | Persistent clients |
| --- | ---: |
| Timed write duration | 92.154 s |
| Throughput | **32.554 writes/s** |
| Commit p50 / p95 / p99 | **95.441 / 669.557 / 2,807.052 ms** |
| Checkpoint attempts / rejected attempts | 4,207 / 207 |
| Proposed delta / equivalent full-image bytes | 4,472,846 / 254,877,260 |
| Journal delta / full / additional checkpoint bytes | 1,327,564 / 59,351 / 0 |
| Newly written object bytes / object count | 13,192,431 / 11,228 |
| Reused objects / reclaimed bytes | 10 / 0 |
| Stored object bytes | 13,192,431 |
| Attempted delta / full replication bytes | 7,923,261 / 40,803,127 |
| Explicit snapshot fallbacks | 2 |
| Provisioned connections per IP / rejected connections | 3,032 / 0 |

Timed writes exclude warmup, verification, and restart. Latencies include client retries. Persistence/replication totals include warmup and aggregate each broker before it stops; they exclude the final full restart. Object bytes plus journal bytes are **14,579,346 bytes** of measured persistence payload/framing—not total device I/O, filesystem metadata, or allocation overhead. Checkpoint rejections are retried coordination attempts, not an observed loss of acknowledged offsets. The large run did not trigger automatic compaction; small-threshold Windows/Linux tests exercise it.

On this Windows/JDK combination directory forcing is unavailable, so automatic object reclamation deliberately retains old history. I do not claim bounded Windows disk usage or Windows power-loss durability. A smaller central journal does not prove lower total disk/network traffic.

## Churn and data-path regressions

The [churn report](2026-09-02-shard-churn.json) also passed **3,000 writes**, **1,000/1,000 exact offsets**, every owner, controller loss, and full restart. It created 6,000 clients across its write/verification phases, retained the 1,000-per-IP limit, and recorded zero admission rejections. Write throughput was **32.985 writes/s** over 90.949 seconds; p50/p95/p99 were **63.531 / 821.382 / 3,440.349 ms**. It recorded 3,136 checkpoint attempts, 136 rejected attempts, 965,566 delta-journal bytes, 27,231 full-journal bytes, and 7,077,047 new object bytes. It did not trigger checkpoint reclamation. This unprofiled run and the profiled persistent run have different warmup and client residency, so they are not a controlled optimization comparison.

The one-million-record regression used the existing single-node plaintext workload: 1 KiB incompressible values, eight partitions, four producers, four consumers, LZ4, `acks=all`, and periodic forcing every 1,000 ms or 64 MiB.

- Produce: **453,336 records/s**, 442.7 MiB/s, 2.206 seconds.
- Consume: **548,857 records/s**, 536.0 MiB/s, 1.822 seconds; **1,000,000/1,000,000 exact records**.
- Maximum acknowledgement latency: **571.137 ms**; histogram p99 at most 1,000 ms.
- Storage: **1,034,136,574 bytes**; peak heap: **1,496.6 MiB**.
- Flush measurement: 10 operations, 694.9 MiB forced, 654.9 ms force time, 291.4 MiB still pending.

This is a cache-assisted default-data-path regression, not synchronous RF=3 durability capacity. Ten million records were not rerun for this milestone.

## Profile and capacity verdict

The successful recording shows substantial shared-lock contention: internal metadata handling reached **2.13 seconds** of observed monitor wait, image installation **1.83 seconds**, and offset commits **1.12 seconds**. Sampled hot methods include collection growth, boxed equality/byte operations, and string serialization. Sampled allocation pressure was **44.13% byte arrays**, **21.11% object arrays**, and **10.58% tuples**. The maximum recorded GC pause was **22.8 ms**; these samples do not attribute every latency spike to one cause.

Correctness passed for the recorded persistent workload. **This is not a production-capacity pass**: 32.554 writes/s with a 2.807-second p99, on one profiled shared host, does not establish a Kafka-replacement capacity envelope. It also does not establish a controlled speedup over older churn-heavy runs. The next capacity work remains independent shard publication/consensus, finer-grained service locking, and reducing whole-image serialization/validation, with representative membership and transaction load on dedicated hosts.

## Rolling upgrade evidence

Every campaign built the actual pinned historical source and passed all ten phases, **40/40 ordered records**, and exact committed offset **40**. Each preserved its baseline feature map during partial upgrade, accepted rollback before activation, activated the current feature map only after every voter upgraded, and rejected unsafe downgrade afterward.

| Baseline | Elapsed campaign time | Archived report |
| --- | ---: | --- |
| Format 10 / 1.2.0, `fbe4e98a7b9c55680a365453041ee084c0a6efbf` | 9.559 s | [JSON](2026-09-02-shard-rolling-format10.json) |
| Format 9 source predecessor, `8c8ff4cc63955d23eb566683bc1ac473e767bae3` | 8.877 s | [JSON](2026-09-02-shard-rolling-format9.json) |
| 1.0.0, `c61264bf304719403b77c9b60709801be544373e` | 8.953 s | [JSON](2026-09-02-shard-rolling-1.0.0.json) |

The current application jar SHA-256 is `7392e74b0aa7ed0690d74190cb5a7bf53f7acfb4df1d65451d3957b73050798b`. The rolling reports' `+working-tree` marker refers only to the evidence/documentation changes over the tested implementation revision. These are source-binary campaigns, not published-image qualification. Rolling traffic remains non-idempotent `acks=all` because historical binaries restrict anonymous producer initialization; current-binary idempotence is tested separately.

Raw local evidence remains in `artifacts/shard-final-regressions.log`, `shard-storage-linux-final.log`, `shard-client-matrix-final.log`, the three rolling logs, and `shard-persistent-final.jfr`. The failed initial persistent runs and the pre-fix heartbeat regression are retained separately. The committed JSON files above are the successful campaign reports; full logs/recordings are not checked into Git.

## Remaining release gates

The milestone does not execute a 72-hour authenticated multi-tenant soak, physical power/device cuts, arbitrary external packet impairment, cross-host coordinated restore drills, or a dedicated-host RF=3 capacity campaign. Source-binary rolling checks do not qualify a published Docker image pair. Administrative consumer-group APIs, later new-consumer protocol versions, broader compaction codecs, and coordinated cross-host snapshot lifecycle remain separate roadmap items.

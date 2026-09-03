# Coordinator batching qualification — 2026-09-03

I qualified the broker changes in `f0b837010603a883f94e488bb453ef4c53e82320`, version **1.3.0-SNAPSHOT**. The complete final Scala test run used `6253441a80c8334ef4e918ab9fa49a29a430a7ac`, which changes documentation, CI, and a test assertion but not broker code. The final evidence commit also adds a Go download fallback to the external test runner; it does not change the broker or publish an image.

The staged application JAR SHA-256 is `de0228471d837649bbdbaaef65eb8ea5cfff4b1335c83a2695e8d287bedd5331`. Rolling reports retain their `+working-tree` marker because documentation/evidence and that test-runner option were being prepared during qualification. Their application hashes match this tested artifact.

## What changed

Ordinary clustered OffsetCommit requests now use one bounded FIFO worker per broker. Count and estimated-byte caps include queued and in-flight requests; each publication batch has separate bounds. Commands revalidate ownership/readiness and member identity before staging, and valid commands share one atomic checkpoint. Failed publication restores authoritative state. Deadlines cancel work only before staging; interruption and shutdown drain active publication without abandoning the online-snapshot barrier. Transactional offset/outcome checkpoints and single-node local journal writes keep their existing paths.

I also made offset validation and mutation one critical section, prevented readers from observing staged offsets, added a single locked OffsetFetch view, exposed eleven node-only metrics and read-only Kafka config values, added pressure alerts, and extended CI with matched capacity controls. The [batching runbook](../offset-batching.md) defines exact limits, errors, lifecycle behavior, and remaining architectural constraints.

## Defect found by the control campaign

The first large single-request control failed initial verification with `wrong recovered offset for group 0: None`, despite successful commits and all three brokers reporting metadata version 3005. The old OffsetFetch handler checked readiness separately while collecting values and while encoding errors. A false-to-true readiness transition could skip the lookup but encode success, making a transiently unavailable result look like a missing offset.

The fix captures readiness and all requested values once under the service lock and reuses that decision for every response error field. A deterministic changing-readiness regression verifies that unavailable reads cannot become successful missing offsets. The four corrected persistent campaigns and the churn campaign below all pass exact offset verification through failover and restart. I retain the failed run in the local raw log `artifacts/batching-full-and-controls.log`; I do not count its incomplete performance result as a pass. The earlier full 412-test run did not cover this race; the final suite has 414 tests.

## Matched persistent-client capacity

Each run uses 1,000 long-lived Kafka 4.3.1 classic consumers with manual assignment, 1,000 excluded warmup writes, 3,000 timed writes, all three coordinator owners, controller shutdown, majority operation, and complete broker restart. Every final offset is checked exactly. One request per client is active at a time. Default batching is 64 requests/1 MiB with 2 ms linger; the matched control uses one request with no linger. Both use the same bounded queue and publication safeguards.

| Workers | Mode | Writes/s | Write seconds | p50 ms | p95 ms | p99 ms | Exact final offsets |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 8 | [Single-request control](2026-09-03-offset-control-8.json) | 20.302 | 147.769 | 123.144 | 1,400.299 | 4,278.921 | 1,000/1,000 |
| 8 | [Bounded batching](2026-09-03-offset-persistent-8.json) | 24.480 | 122.547 | 111.813 | 1,191.376 | 3,320.695 | 1,000/1,000 |
| 32 | [Single-request control](2026-09-03-offset-control-32.json) | 26.137 | 114.780 | 327.999 | 5,380.524 | 12,003.948 | 1,000/1,000 |
| 32 | [Bounded batching](2026-09-03-offset-persistent-32.json) | 41.922 | 71.561 | 297.824 | 3,309.285 | 6,341.595 | 1,000/1,000 |

The measured throughput improvement is **20.6% at 8 workers** and **60.4% at 32 workers**. The 32-worker p99 improves by about **47.2%**, but remains over six seconds. These are single paired observations, not repeated-run confidence intervals or production SLOs. An earlier pre-fix batched run reached 32.727 writes/s at eight workers; the variation is another reason not to extrapolate these measurements to deployment capacity.

| Workers / mode | Dispatched requests / batches | Admitted requests ending in errors | Checkpoint attempts / failures | Peak retained requests / estimated bytes |
| --- | ---: | ---: | ---: | ---: |
| 8 / control | 4,869 / 4,869 | 869 | 4,039 / 39 | 8 / 4,972 |
| 8 / batching | 4,783 / 3,696 | 783 | 3,147 / 32 | 8 / 4,976 |
| 32 / control | 6,906 / 6,906 | 2,906 | 4,061 / 61 | 32 / 19,904 |
| 32 / batching | 6,156 / 2,591 | 2,156 | 1,984 / 126 | 23 / 14,306 |

These counters include warmup and retries; timed write latency excludes warmup and includes client retries. Pre-admission handler errors are not batch counters. Batches rejected for readiness/member validation need not reach a checkpoint. At 32 workers, 35.0% of dispatched/admitted requests still end in errors before client retries succeed. Batching amortizes work but can also couple conflict retries across commands. **Zero queue/connection admission rejections is not zero retry pressure.**

The [8-worker churn campaign](2026-09-03-offset-churn-8.json) separately creates 6,000 clients, commits 3,000 writes, and verifies 1,000 exact offsets through failover/restart. It measures **44.307 writes/s**, p50 **43.826 ms**, p95 **640.203 ms**, and p99 **2,048.818 ms**, with zero admission rejections. Its first write phase grows cardinality from empty and has no persistent warmup, so I do not treat its throughput as a matched comparison against warmed persistent clients.

## Environment and durability

- Same Windows development host as the previous report: Intel Core i5-12450HX, 12 logical processors, 21.72 GiB OS-visible RAM, WD PC SN5000S 512 GB SSD with NTFS; Java 21.0.11, Scala 3.3.8, Kafka client 4.3.1.
- Three in-process brokers with separate temporary directories on the same physical device; RF=3, min ISR=2, synchronous persistence, format-11 shard objects. This is not a dedicated-host or independent-device RF=3 benchmark.
- The qualification fixture retains its aggressive peer timeout of 300 ms, heartbeat 100 ms, and election timeout 600 ms. These are test settings, not deployment recommendations. Client request/API timeouts remain 5/30 seconds. Warning logs and client retry overhead are included.
- Persistent runs provision 3,032 connections per IP; churn retains 1,000. Pending limits stay at 1,024 commands / 16 MiB estimated command bytes. Every campaign checks zero connection admission rejection again after restart.
- Windows reports zero object reclamation when directory forcing is unsupported. Retained object history and full-state CPU work remain known limits; no disk-bound or linear horizontal-scaling claim is made.

## Regression and fault evidence

- Complete unit, integration, Kafka-Java end-to-end, security, fault, storage, deployment, and recovery suite: **414/414 passed**, **149 seconds**; `stage` completed.
- Focused final offset isolation, quorum/snapshot, and TCP broker suites: **21/21 passed**. Tests include FIFO rewinds, invalid-command isolation, all-or-none rollback, no pre-publication reads, static replacement while queued, byte/count limits, deadlines, interruption, ownership checks, shutdown drain, and worker errors.
- A real three-broker wire batch receives no successful acknowledgements when follower metadata publication is blocked; retry and restart recover all eight groups exactly. A separate reply-barrier test takes an online snapshot during publication, waits for its acknowledgement boundary, restores the snapshot, and checks the exact committed offset.
- Linux storage/batching: **51/51 passed**. The strict directory probe reports `directory_force=true`, **134 objects / 8,452 bytes reclaimed**, and **97 bytes retained**. The filesystem is disposable Linux `/tmp` (tmpfs), not a physical power-loss qualification.
- Default data-path load: **1,000,000 / 1,000,000 exact records**, **493,225 produced records/s** and **489,511 consumed records/s**. Eight partitions, four producers/four consumers, 1 KiB deterministic payloads, LZ4, single-node plaintext, periodic 1 s / 64 MiB flushing. Maximum acknowledgement latency **697.506 ms**, coarse p99 bucket **<=1,000 ms**, peak heap **1,536 MiB**, stored **986.2 MiB**. This is not RF=3 synchronous or TLS capacity evidence.

## External clients and deployment checks

Java Kafka 4.3.1, KafkaJS 2.2.4, Python confluent-kafka 2.15.0, franz-go 1.21.0, and Confluent.Kafka .NET 2.15.0 each verified **25/25** records against the staged broker in a pinned Linux Temurin JDK container. Java recovered the same records after restart. The broker ran as UID/GID 65532 with a read-only root, dropped capabilities, a data mount, and a 2 GiB memory limit. No protocol errors were observed. Test broker containers were removed; synthetic data remains in local `artifacts/client-data-*` directories.

The first external run stopped at Go because `proxy.golang.org` returned HTTP 403. The complete rerun used `-GoProxy direct` and passed without disabling pinned checksums or TLS. Local Node 26.3.0 still emits KafkaJS's negative-timer warning; this small smoke does not establish broad Node-version support.

Kubernetes renders **1,095 lines**, dashboard JSON parses, PowerShell syntax checks pass, and both GitHub workflows pass Actionlint 1.7.7. The final documentation/release-artifact suite also passes **5/5** tests. **Dockerfile validation is blocked**, not passed: two attempts to resolve `gcr.io/distroless/base-debian12:nonroot` returned HTTP 403. The staged JDK check is not a new distroless image build, multi-architecture release, or Docker Hub publication. Source remains 1.3.0-SNAPSHOT and deployment pins remain at the previously qualified local 1.2.0 image.

## Rolling source-pair qualification

All three campaigns build pinned historical source and the current runtime, rotate three real broker processes through ten phases, verify **40/40 records and committed offset 40**, roll back a partially upgraded cluster before feature activation, activate the new feature floor, reject an unsafe downgrade, and resume exact traffic afterward.

| Baseline | Pinned source | Campaign seconds | Result |
| --- | --- | ---: | --- |
| [Format 10 / 1.2.0](2026-09-03-offset-rolling-format10.json) | `fbe4e98a7b9c55680a365453041ee084c0a6efbf` | 10.112 | Passed |
| [Format 9 / version string 1.1.0](2026-09-03-offset-rolling-format9.json) | `8c8ff4cc63955d23eb566683bc1ac473e767bae3` | 10.112 | Passed |
| [1.0.0](2026-09-03-offset-rolling-1.0.0.json) | `c61264bf304719403b77c9b60709801be544373e` | 10.087 | Passed |

The format-9 source baseline is not the published format-8 1.1.0 image. Rolling traffic remains non-idempotent because the oldest source has the historical anonymous-producer initialization restriction; current idempotence and transactions are covered separately by the full suite. These small source-pair campaigns are not high-load upgrade, broad client-version, or published-image qualification. Temporary historical worktrees and test brokers were cleaned up.

## Remaining release gates

The implementation and documented correctness workload pass. Cascade is **not production ready**. Independent per-shard consensus/publication, finer-grained service locks, less whole-state serialization, high-cardinality membership/transaction churn, dedicated-host capacity, multi-day authenticated soak, physical power/device-loss tests, arbitrary network impairment, coordinated multi-host restore drills, broader client/API coverage, and the published format-8 image upgrade pair still need evidence. This milestone neither implements nor silently waives those gates.

Raw local evidence is retained under `artifacts/batching-final-capacity.log`, `batching-final-regressions.log`, `batching-linux.log`, `batching-external-clients-direct.log`, `batching-docker-check.log`, and `batching-rolling-*.log`. The checked-in JSON reports preserve their original revisions, timestamps, and measured counters.

# Coordinator publication qualification — 2026-09-05

## Result

The bounded controller publication milestone passes its local correctness and recovery gates. The 1,000-group campaign acknowledged 3,000 writes, verified every final offset, used all three coordinator owners, survived controller loss, and recovered every offset after restarting all three brokers from their existing disks.

The controller received 1,675 publication requests and dispatched 1,156 batches. It committed 1,521 proposals in 1,092 successful quorum batches, a reduction of 429 quorum publications (28.2%) relative to publishing those successful requests individually. It rejected 154 conflicting proposals before publication; Kafka clients retried them, and exact final state passed. No proposal was rejected by the bounded queue. Peak retained publication work was four requests and 22,599 encoded bytes, below limits of 1,024 requests and 64 MiB.

This is not a throughput pass. The measured write phase achieved 57.934 writes/s with 205.729 ms p50, 2,237.835 ms p95, and 4,714.624 ms p99. It is not a controlled comparison with earlier runs because this campaign uses a five-millisecond publication linger and a different source revision. Client logs contain expected retriable `NOT_COORDINATOR` and `COORDINATOR_NOT_AVAILABLE` responses; 153 of 1,673 coordinator checkpoint attempts failed before retries completed. Independent shard consensus, shared-lock removal, and dedicated-host capacity remain open.

## Workload and host

- Source revision: `2af3c23a4eb0d8df434ed8b281442ef04bc9e131`, with a clean worktree when the campaign began.
- Version reported by the source build: 1.3.1. This post-release source milestone is not present in the already-published 1.3.1 container.
- Java 21.0.11, Scala 3.3.8, Kafka Java client 4.3.1.
- Windows 11 Pro 10.0.26200, 12th Gen Intel Core i5-12450HX, eight cores / twelve logical processors, 23,320,190,976 bytes physical memory.
- Three in-process brokers with separate temporary data directories, synchronous flush, RF=3 metadata quorum, 1,000 persistent classic-group clients, 32 workers, two measured writes per group plus one post-failover write.
- Offset batches: 64 requests, 2 ms linger. Controller publication batches: 64 requests, 5 ms linger.
- `write_seconds` excludes warmup, verification, failover, and restart. It includes Kafka client request/retry time. Percentiles cover successful `commitSync` calls after internal retries.

The machine also ran the desktop environment and Docker during the campaign. These numbers are development-host observations, not production SLOs or dedicated-host RF=3 capacity.

## Regression evidence

| Gate | Result |
| --- | --- |
| Complete Scala unit/integration/end-to-end suite | 456/456 passed in 187 seconds |
| Focused publication queue/shard/quorum suites | 13/13 passed, including twelve disjoint groups across three owners and full restart |
| Fail-closed image-security policy tests | 32/32 passed |
| Dockerfile, Compose, and Kubernetes validation | Dockerfile check clean; two Compose configurations valid; Kubernetes rendered 1,095 lines |
| Five-language staged runtime | Java Kafka 4.3.1, KafkaJS 2.2.4, Python Confluent 2.15.0, franz-go 1.21.0, and Confluent.Kafka .NET 2.15.0 each verified 25/25 records; Java verified 25/25 after restart |

The first external-client attempt stopped when `proxy.golang.org` returned truncated Go dependency archives. I retained that failed log and reran the complete matrix with the previously qualified Go binary. Its SHA-256 was `782d11023f30b70575fdeb2c9019fc40c447067fcc4bf61eb74399b5a85d8473`, and the pinned Go source had not changed since that build. The rerun passed all languages. This distinguishes an external dependency-download failure from a broker pass.

Raw tracked evidence: [JSON report](2026-09-05-coordinator-publication.json). Local ignored logs are `artifacts/coordinator-publication-1000.log`, `coordinator-publication-full-test.log`, `coordinator-publication-focused.log`, `coordinator-publication-quorum.log`, `coordinator-publication-security-gate.log`, `coordinator-publication-docker-check.log`, and `coordinator-publication-client-matrix-final.log`.

## Reproduce

```text
./sbt "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --batch-max-requests 64 --batch-linger-ms 2 --publication-max-requests 64 --publication-linger-ms 5 --report artifacts/coordinator-publication-1000.json"
./sbt test
node --test scripts/qualify-image-security.test.mjs
```

I follow the [publication safety contract](../coordinator-publication.md) and keep the remaining blockers in the [production-readiness checklist](../production-readiness.md).

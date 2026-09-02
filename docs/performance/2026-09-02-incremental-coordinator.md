# Incremental coordinator qualification — 2026-09-02

I tested implementation revision `fbe4e98a7b9c55680a365453041ee084c0a6efbf`, version **1.2.0**. The following documentation commit does not change the tested source. I archive the [coordinator report](2026-09-02-incremental-coordinator.json), [format-9 rolling report](2026-09-02-incremental-rolling-format9.json), and [1.0.0 rolling report](2026-09-02-incremental-rolling-1.0.0.json).

## What changed

Consecutive coordinator-only updates now use atomic delta records in the forced metadata journal and the existing quorum replication path. Deltas require exact versions, controller term, touched-shard versions, and a SHA-256 fingerprint of their base. Missing-base followers reject the delta and receive a full snapshot. Structural changes and periodic checkpoints keep complete images. A unanimously supported feature raises the storage floor to format 10 before activation.

This reduces steady-state journal/transfer bytes. It does **not** create independent shard consensus, remove shared locks, or eliminate full-image encoding for fingerprinting and state comparison. The [runbook](../incremental-coordinator.md) describes those boundaries and the counters.

Qualification found and fixed three defects: divergent failed-quorum histories could share numeric base versions; anonymous idempotent producer initialization was incorrectly restricted to the controller; and Metadata v5-v8 responses omitted version-specific fields. Regression tests cover exact-history rejection, producer initialization through every broker, and every advertised Metadata version (v4-v12) using Kafka's generated request/response classes. The field boundaries follow [Kafka's Metadata response schema](https://raw.githubusercontent.com/apache/kafka/4.3.1/clients/src/main/resources/common/message/MetadataResponse.json).

## Acceptance results

| Check | Result |
| --- | --- |
| Full unit/integration/Kafka-Java/security/fault suite | **360/360 passed**, 130 seconds |
| Focused Metadata-version and anonymous-producer regressions | **4/4 passed**, then repeated within the full suite |
| Coordinator campaign | **3,000 writes**, **1,000/1,000** exact final group offsets |
| Controller failover and complete broker restart | Passed; all three coordinator owners exercised |
| Format-9 source predecessor to 1.2.0 | 10 phases, **40/40 records**, committed offset **40**, 8.933 seconds |
| Pinned 1.0.0 to 1.2.0 | 10 phases, **40/40 records**, committed offset **40**, 8.653 seconds |
| Local container language matrix | Java, KafkaJS, Python, Go, and .NET: **25/25 records each** |
| Container volume recovery | Java verified the same **25/25** records after restart |
| One-million-record data path | **1,000,000/1,000,000** consumed |

The full suite includes torn-tail/CRC recovery, wrong-base startup rejection without evidence truncation, checkpoint replacement, duplicate delta delivery, lagging-follower snapshot fallback, dropped acknowledgements, quorum loss, and transaction/offset recovery. Automatic compaction did not trigger in the large campaign; focused small-threshold tests exercise it.

## Coordinator measurements

Environment: Windows/NTFS, Java 21.0.11, Intel Core i5-12450HX (8 cores/12 logical processors), 23,320,190,976 bytes of physical memory, WD PC SN5000S 512 GB SSD. Three in-process brokers use separate directories on that same SSD, RF=3, minimum ISR=2, and synchronous metadata durability. The fault fixture uses 300 ms peer timeouts, 100 ms heartbeats, and a 600 ms election timeout—not a production timing recommendation.

The workload uses 1,000 groups, eight workers, two offset writes per group before controller loss, one afterward, and exact verification before/after failover and after restarting every broker. Kafka 4.3.1 consumers are created and closed per group in each phase. This is offset/client churn, not a long-lived group membership or transaction benchmark.

| Measurement | Result |
| --- | ---: |
| Write-phase time, including client churn | 116.896 s |
| Write throughput | **25.664 writes/s** |
| Commit p50 / p95 / p99, including retries | **62.170 / 1,172.215 / 4,103.894 ms** |
| Checkpoint attempts / rejected attempts | 3,060 / 60 |
| Proposed delta bytes / equivalent full-state bytes | 2,581,319 / 142,490,799 |
| Actual forced delta journal bytes | 6,887,762 |
| Actual forced full-image journal bytes | 645,055 |
| Additional checkpoint bytes | 0 |
| Attempted delta replication bytes | 4,211,627 |
| Attempted full replication/snapshot bytes | 5,017,949 |
| Explicit snapshot fallbacks | 0 |

The proposal encoding is about 98.2% smaller than its equivalent full-state encoding; that is **not** a measured 98.2% reduction in total disk or network traffic. Journal counters include framing and successfully forced writes. Replication counters include RPC bodies, retries, and snapshot responses, but not TCP/TLS framing. Totals aggregate each broker before it stops and exclude the final all-broker restart. The separate deterministic fallback tests exercise the path even though this run needed no explicit fallback.

Correctness passed; performance is **not a production-capacity pass**. I observed 14,554 TIME_WAIT sockets during the run; this host has 13,977 configured IPv4 dynamic ports starting at 1024. Endpoint tuples can reuse port numbers, so this does not by itself prove exhaustion, but it confirms substantial connection churn. I did not alter the OS socket settings. The shared quorum, full-state CPU work, short fixture timeouts, and client churn all remain relevant. The previous milestone measured 30.455 writes/s on this host; these non-controlled runs do not demonstrate a throughput improvement. I need dedicated-host measurements with long-lived clients and representative membership/transaction load.

## Data-path regression

The default single-node plaintext workload used 1 KiB incompressible values, eight partitions, four producers, four consumers, LZ4, `acks=all`, periodic flushing every 1,000 ms or 64 MiB, and one million records.

- Produce: **468,221 records/s**, 457.2 MiB/s, 2.136 seconds.
- Consume: **515,511 records/s**, 503.4 MiB/s, 1.940 seconds; exact count **1,000,000**.
- Maximum acknowledgement latency: **722.591 ms**; bucketed p99 at most 1,000 ms.
- Storage: **1,034,136,922 bytes** (986.2 MiB); peak heap: **1,647.6 MiB**.
- At the flush measurement: 12 force operations, 736.0 MiB forced, 525.1 ms force time, 250.2 MiB pending.

This cache-assisted, periodically flushed single-node run is an exact-count/hot-path regression, not RF=3 durability capacity. The ten-million result remains historical; I did not rerun ten million in this milestone.

## Rolling and container boundaries

Both rolling campaigns preserve the baseline feature map through partial upgrade and rollback, then activate the new map only after replacing every voter. Both reject unsafe downgrade and recover traffic afterward. The format-9 predecessor is source revision `8c8ff4cc63955d23eb566683bc1ac473e767bae3`, whose version string was 1.1.0; it is **not** the published format-8 Docker Hub image. The published-image adjacent-release pair remains unqualified. Historical-binary traffic uses non-idempotent `acks=all`; current-binary idempotence is tested separately. Cross-version idempotent retries and long-lived transactions remain additional qualification work.

The current application jar SHA-256 in both campaigns is `866955e643cdafbc1be321eb091d01fccdb65ee6569ec22c3570404ebdad8fb6`.

The local Linux/amd64 image has both `miladsade96/cascade:1.2.0` and `cascade:1.2.0` tags, image/index ID `sha256:0d316f03b5b8730213e5bfd72fb314b06859fbef8c8d2f6fb330e6a5ab970921`, and Docker-reported size **39,164,217 bytes**. It was tested with UID/GID 65532, a read-only root, no capabilities, `no-new-privileges`, a writable data volume, and a 2 GiB memory limit. The five pinned clients were Java 4.3.1, KafkaJS 2.2.4, confluent-kafka Python 2.15.0, franz-go 1.21.0, and Confluent.Kafka .NET 2.15.0. The four non-Java scripts also verify committed offsets. KafkaJS emitted a timer warning but completed both assertions.

A registry request for the unpinned runtime tag returned 403. I built using the already-resolved base digests: `eclipse-temurin:21-jdk-jammy@sha256:ce5767b7222312d42395f5bab033cd91f09e44032a2f21bdfd7b5b912dbe1e77` and `gcr.io/distroless/base-debian12:nonroot@sha256:7f0c72cd138b442ae0deeb69c08b1acf5525439ba251a49ad93c320a061567e5`. Go's proxy also returned 403; `GOPROXY=direct` fetched the same pinned dependencies with normal checksum validation. The initial .NET container invocation accidentally included old generated build files; mounting only its project and source files produced a clean passing run. I removed the disposable containers and data volume afterward and retained both image tags. I did not push to Docker Hub, test ARM64, or perform a new vulnerability scan.

## Reproduce and inspect

```text
./sbt test stage
./sbt "Test/runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 8 --rounds 2 --report artifacts/incremental-coordinator-final.json"
./sbt "Test/runMain cascade.performance.LoadTest --records 1000000"
```

I use the [rolling commands](../rolling-upgrades.md) for the pinned binary matrix and the [container guide](../containers.md) for image settings. Local raw evidence is in `artifacts/incremental-complete-suite.log`, `artifacts/incremental-final-capacity.log`, `artifacts/incremental-rolling-*.log`, `artifacts/incremental-*-client.log`, `artifacts/incremental-java-restart.log`, and `artifacts/docker-1.2.0-final-build.log`; these large logs are not committed.

The next capacity work is independent shard persistence/consensus and finer-grained service locking. Multi-day authenticated soak, physical power/device loss, arbitrary packet impairment, recurring restore drills, published-image rolling qualification, and dedicated-host RF=3 capacity remain blocking [production-readiness gates](../production-readiness.md).

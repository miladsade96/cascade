# Reproducible benchmarking

I separate correctness regressions, capacity qualification, and product comparisons because they answer different questions.

## Same-workload Cascade/Kafka comparison

`scripts/compare-kafka.ps1` runs the same `KafkaComparisonBenchmark` client code against Cascade and the official Apache Kafka image. It executes the brokers sequentially on the same host and port so they do not compete for CPU, memory, or disk at the same time.

| Input | Default |
| --- | ---: |
| Measured records | 250,000 |
| Warm-up records | 25,000 |
| Payload | 1,024 deterministic bytes |
| Partitions | 8 |
| Replication factor | 1 |
| Producers | 4 |
| Consumers | 1, explicit partition assignment |
| Compression | LZ4 |
| Acknowledgements | `all` with idempotence |
| Container limit | 4 CPUs, 4 GiB |
| Client | Apache Kafka Java 4.3.1 |

Both brokers receive identical producer properties. The consumer uses explicit assignments, seeks past per-partition warm-up offsets, decodes every measured sequence number, and fails on loss, duplication, or an out-of-range value.

```powershell
./sbt.bat Test/compile
./scripts/compare-kafka.ps1 -CascadeImage miladsade96/cascade:1.8.0 -KafkaImage apache/kafka:4.3.1 -Records 250000 -WarmupRecords 25000 -PayloadBytes 1024 -Partitions 8 -Producers 4
```

Each JSON file records the complete workload, warm-up and measurement durations, produce/consume/end-to-end throughput, p50/p95/p99 acknowledgement latency, exactness, client heap, JDK, Scala, OS, and visible processors. The wrapper also reports container startup time, persisted volume bytes, and a post-run memory/CPU snapshot.

## How I publish results

I record hardware and runtime versions; every workload and durability option; throughput and latency; memory, GC, disk, and force activity when available; startup/failover time; and exact lost/duplicate counts. I publish both products without selecting only favorable metrics.

A shared Docker Desktop development host is useful for regression and implementation comparisons, but it is not production capacity qualification. RF=1 results cannot be generalized to RF=3 synchronous durability.

The latest matched run is [the 2026-09-19 Cascade/Kafka comparison](performance/2026-09-19-kafka-comparison.md). I keep both the metrics where Cascade leads and the metrics where Kafka leads.

`cascade.performance.LoadTest` remains the high-record-count Cascade regression. Its historical results and limitations are in [the heavy-load report](performance/2026-08-05-heavy-load.md).

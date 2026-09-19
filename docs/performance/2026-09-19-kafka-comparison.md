# Cascade and Apache Kafka comparison — 2026-09-19

I ran both brokers sequentially with the same Java client code, workload, host port, container limits, and exact sequence verifier. I report the metrics that favor either product.

## Workload

| Setting | Value |
| --- | --- |
| Cascade image | `sha256:e212128215726b8280fed0e142ba92496c52b663e20c3df4bb5819d15746448d` from `e9a6060` |
| Apache Kafka image | `apache/kafka:4.3.1` (`sha256:77e3df9054047a88b520d0cc46e16696d3b22022e1d580aeccd2632df6532837`) |
| Kafka client | Apache Kafka Java 4.3.1 |
| Measured / warm-up | 250,000 / 25,000 records |
| Payload | 1,024 deterministic bytes with an encoded sequence number |
| Topology | 8 partitions, replication factor 1 |
| Clients | 4 concurrent idempotent producers, 1 explicit-assignment consumer |
| Producer settings | LZ4, `acks=all`, 128 KiB batch, 5 ms linger |
| Container limit | 4 CPUs, 4 GiB RAM, sequential execution |
| Client runtime | Eclipse Temurin 21.0.11, 12 visible processors |

## Results

| Measurement | Cascade | Apache Kafka 4.3.1 |
| --- | ---: | ---: |
| Startup to open TCP port | 409 ms | 408 ms |
| Warm-up | 528 ms | 713 ms |
| Produce time | 1,903 ms | 2,449 ms |
| Produce throughput | 131,371.519 records/s | 102,082.483 records/s |
| Consume time | 2,981 ms | 2,402 ms |
| Consume throughput | 83,864.475 records/s | 104,079.933 records/s |
| End-to-end throughput | 51,187.551 records/s | 51,535.766 records/s |
| Ack p50 | 697.986 ms | 265.226 ms |
| Ack p95 | 915.771 ms | 638.016 ms |
| Ack p99 | 949.107 ms | 710.202 ms |
| Consumed / lost / unexpected duplicates | 250,000 / 0 / 0 | 250,000 / 0 / 0 |
| Client heap after verification | 270.086 MiB | 258.893 MiB |
| Broker memory after run | 349.6 MiB | 559.8 MiB |
| Post-run CPU snapshot | 0.54% | 3.46% |
| Persisted volume bytes | 284,094,464 | 284,295,168 |

Cascade produced about 29% faster and used about 38% less broker memory in this run. Kafka consumed about 24% faster, finished end to end about 0.7% faster, and had lower acknowledgement latency and slightly lower client heap after verification. Both products preserved every measured sequence number exactly once.

```powershell
./sbt.bat Test/compile
./scripts/compare-kafka.ps1 -CascadeImage miladsade96/cascade:1.8.0 -KafkaImage apache/kafka:4.3.1 -Records 250000 -WarmupRecords 25000 -PayloadBytes 1024 -Partitions 8 -Producers 4 -Java "$env:JAVA_HOME\bin\java.exe"
```

## Host and limitations

The run used a Lenovo 83GS laptop with an Intel Core i5-12450HX (8 cores, 12 logical processors), about 21.7 GiB host RAM, an NVMe system disk, Windows 11 Pro build 26200, Docker Desktop 29.8.0, and WSL2 kernel 6.6.87.2. Docker reported 12 CPUs and about 10.56 GiB available to its VM.

This is one development-host sample, not a statistically repeated benchmark. RF=1 removes replication cost; plaintext removes TLS cost; the products do not have identical storage implementations or flush defaults; the client runs outside the broker cgroup; the heap values are post-run rather than peak; CPU is a post-run snapshot rather than an integrated measurement; and TCP-open startup is not full application readiness. The asynchronous acknowledgement latency includes client queueing behind the entire submitted workload. I do not extrapolate these results to RF=3, dedicated hardware, sustained workloads, tail latency under saturation, or production capacity.

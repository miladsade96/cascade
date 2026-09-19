# Cascade and Apache Kafka comparison — 2026-09-19

I ran both brokers sequentially with the same Java client code, workload, host port, container limits, and exact sequence verifier. I report the metrics that favor either product.

## Workload

| Setting | Value |
| --- | --- |
| Cascade candidate | `sha256:975dd1cf77f74c0b4fc6d2a6ce447ea67fb665e1df272b82982f52d52976432e` from `7e307bb` |
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
| Startup to open TCP port | 420 ms | 389 ms |
| Warm-up | 530 ms | 896 ms |
| Produce time | 1,883 ms | 2,108 ms |
| Produce throughput | 132,766.861 records/s | 118,595.825 records/s |
| Consume time | 2,612 ms | 2,202 ms |
| Consume throughput | 95,712.098 records/s | 113,533.152 records/s |
| End-to-end throughput | 55,617.353 records/s | 58,004.640 records/s |
| Ack p50 | 665.462 ms | 267.691 ms |
| Ack p95 | 854.053 ms | 422.762 ms |
| Ack p99 | 896.734 ms | 441.928 ms |
| Consumed / lost / unexpected duplicates | 250,000 / 0 / 0 | 250,000 / 0 / 0 |
| Client heap after verification | 464.498 MiB | 280.913 MiB |
| Broker memory after run | 357.7 MiB | 564.2 MiB |
| Post-run CPU snapshot | 0.55% | 2.11% |
| Persisted volume bytes | 284,102,656 | 284,278,784 |

Cascade produced about 12% faster and used about 37% less broker memory in this run. Kafka consumed about 19% faster, finished end to end about 4.3% faster, and had substantially lower acknowledgement latency and lower client heap after verification. Both products preserved every measured sequence number exactly once.

```powershell
./sbt.bat Test/compile
./scripts/compare-kafka.ps1 -CascadeImage miladsade96/cascade:1.8.0-candidate -KafkaImage apache/kafka:4.3.1 -Records 250000 -WarmupRecords 25000 -PayloadBytes 1024 -Partitions 8 -Producers 4 -Java "$env:JAVA_HOME\bin\java.exe"
```

## Host and limitations

The run used a Lenovo 83GS laptop with an Intel Core i5-12450HX (8 cores, 12 logical processors), about 21.7 GiB host RAM, an NVMe system disk, Windows 11 Pro build 26200, Docker Desktop 29.8.0, and WSL2 kernel 6.6.87.2. Docker reported 12 CPUs and about 10.56 GiB available to its VM.

This is one development-host sample, not a statistically repeated benchmark. RF=1 removes replication cost; plaintext removes TLS cost; the products do not have identical storage implementations or flush defaults; the client runs outside the broker cgroup; the heap values are post-run rather than peak; CPU is a post-run snapshot rather than an integrated measurement; and TCP-open startup is not full application readiness. The asynchronous acknowledgement latency includes client queueing behind the entire submitted workload. I do not extrapolate these results to RF=3, dedicated hardware, sustained workloads, tail latency under saturation, or production capacity.

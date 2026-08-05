# Heavy-load report - 2026-08-05

## Outcome

Cascade passed exact produce/consume verification at both one million and ten million records after the Fetch and flush-path corrections.

On the ten-million-record workload, batched background flushing improved produce throughput from **57,400 to 182,285 records/s**, a **3.18x improvement**. Produce time fell from 174.2 to 54.9 seconds. All **10,000,000 / 10,000,000** records were consumed without gaps.

The remaining sustained write limit is storage-bound on this machine: 9.58 GiB was explicitly forced in 47.6 cumulative seconds while the write phase lasted 54.9 seconds. The fix removes pathological per-request forces and partition-lock stalls, but it cannot make the underlying storage device persist bytes faster.

## Environment

- Windows, Java 21.0.11, Scala 3.3.8
- 8 physical cores / 12 logical processors
- Broker and Kafka 4.3.1 clients in the same JVM
- Storage on the local system-temp volume (`C:`)
- Kafka client 4.3.1

These are development-machine regression measurements, not production capacity figures. The shared JVM and filesystem cache affect throughput, latency, CPU, and heap results.

## Workload

```text
records             10,000,000
payload             1,024 bytes, deterministic incompressible variants
partitions          8
producer clients    4
consumer clients    4
compression         LZ4
acks                all
batch.size          128 KiB
linger.ms           5
segment size        256 MiB
flush policy        periodic
flush interval      1,000 ms
flush byte limit    64 MiB per partition
```

Reproduction command:

```bash
./sbt "Test/runMain cascade.performance.LoadTest --records 10000000 --payload-bytes 1024 --partitions 8 --producers 4 --consumers 4 --compression lz4 --flush-policy periodic --flush-interval-ms 1000 --flush-bytes 67108864"
```

## Ten-million-record comparison

| Metric | Per-request force | Batched background force |
| --- | ---: | ---: |
| Produce throughput | 57,400 records/s | **182,285 records/s** |
| Produce payload throughput | 56.1 MiB/s | **178.0 MiB/s** |
| Produce elapsed | 174.217 s | **54.859 s** |
| Produce CPU | 0.64 cores | 1.75 cores |
| Ack latency p50 | >5,000 ms | <=1,000 ms |
| Ack latency p95 | >5,000 ms | >5,000 ms |
| Ack latency max | 32,144.378 ms | **19,017.787 ms** |
| Explicit force operations | one per nonzero-ack append | 131 |
| Forced data during produce | not instrumented | 9,805.2 MiB |
| Time inside force operations | not instrumented | 47,624.5 ms |
| Pending force data at measurement | not instrumented | 57.2 MiB |
| Consumer verification | 10,000,000 passed | 10,000,000 passed |
| Consume throughput | 561,307 records/s | 473,058 records/s |
| Consume elapsed | 17.816 s | 21.139 s |
| Peak shared-JVM heap | 5,321.6 MiB | 5,221.3 MiB |

The acknowledgement histogram begins at each asynchronous producer `send`, so saturation values include client-side queueing. Although maximum latency improved substantially, the p95 remains above five seconds because the producers offer data faster than this disk can persist it for a sustained ten-gigabyte run.

## One-million calibration

The final periodic-flush calibration completed with:

| Metric | Result |
| --- | ---: |
| Produce throughput | 614,413 records/s / 600.0 MiB/s |
| Produce elapsed | 1.628 s |
| Ack latency max | 449.623 ms |
| Force operations during produce | 8 |
| Consume throughput | 556,232 records/s / 543.2 MiB/s |
| Consumer verification | 1,000,000 / 1,000,000 passed |
| Peak shared-JVM heap | 1,259.0 MiB |

This faster short run benefits from the filesystem cache and must not be extrapolated as sustained disk throughput.

## Flush-path correction

The old Produce path passed `force = acknowledgements != 0` to every partition append. Consequently, both `acks=1` and `acks=all` performed synchronous `FileChannel.force(false)` while holding the partition lock.

The corrected design:

1. Treats Kafka acknowledgement level independently from local fsync policy.
2. Defaults to a periodic policy with one-second and 64 MiB thresholds.
3. Uses one broker-wide scheduled flusher instead of one thread per partition.
4. Snapshots dirty generations under the partition lock, then performs `force(false)` outside that lock.
5. Schedules an immediate background pass at the byte threshold or segment rollover.
6. Forces remaining dirty segments during clean shutdown.
7. Retains an explicit `sync` policy for strict per-append local persistence.
8. Recovers an incomplete batch tail and drops later offset-dependent segments when necessary.

## Fetch correction

The earlier load test also found a non-contiguous Fetch pagination defect. Fetch now stops at the first non-fitting batch after a response has begun while preserving Kafka's first-batch size exception. Repeated one-million and ten-million runs consumed every persisted record after that correction.

## Verification

`sbt test` passed **15/15** tests:

- Wire codec unit tests
- Fetch pagination regression
- Periodic and synchronous flush-policy tests
- Broker-wide background flush test
- Active and multi-segment crash-tail recovery tests
- Raw-socket Kafka protocol integration test, including proof that `acks=1` does not force inline
- Kafka 4.3.1 Admin/Producer/Consumer end-to-end test

## Verdict

The pathological flush implementation is fixed and correctness is good. Sustained write performance improved materially, but this development machine remains limited to roughly 178 MiB/s under explicit periodic persistence. Lower latency or higher sustained throughput now requires a faster/dedicated data device, multiple physical log devices, reduced flush guarantees, or replicated brokers that use Kafka-style ISR durability.

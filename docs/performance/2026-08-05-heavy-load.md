# Heavy-load test - 2026-08-05

## Result

After fixing the Fetch and flush paths, I ran exact produce and consume checks at one million and ten million records. Both runs passed.

In the ten-million-record test, my background batch-flushing change increased produce throughput from **57,400 to 182,285 records/s**. That's a **3.18x improvement**. Produce time dropped from 174.2 seconds to 54.9 seconds, and Cascade consumed all **10,000,000 / 10,000,000** records without gaps.

At this point, the drive is the sustained write limit on my machine. Cascade explicitly forced 9.58 GiB in 47.6 cumulative seconds during a 54.9-second write phase. I removed the unnecessary per-request forces and partition-lock stalls, but I can't make the disk persist data faster than its physical limit.

## Machine and setup

- Windows, Java 21.0.11, Scala 3.3.8
- 8 physical cores and 12 logical processors
- Broker and Kafka 4.3.1 clients running in the same JVM
- Data stored on the local system temporary volume (`C:`)
- Kafka client 4.3.1

I use these numbers for development regression testing, not as production capacity claims. The shared JVM and filesystem cache affect throughput, latency, CPU, and heap usage.

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

Run the same test with:

```bash
./sbt "Test/runMain cascade.performance.LoadTest --records 10000000 --payload-bytes 1024 --partitions 8 --producers 4 --consumers 4 --compression lz4 --flush-policy periodic --flush-interval-ms 1000 --flush-bytes 67108864"
```

## Ten-million-record comparison

| Metric | Old per-request force | New background batch force |
| --- | ---: | ---: |
| Produce throughput | 57,400 records/s | **182,285 records/s** |
| Produce payload throughput | 56.1 MiB/s | **178.0 MiB/s** |
| Produce elapsed | 174.217 s | **54.859 s** |
| Produce CPU | 0.64 cores | 1.75 cores |
| Ack latency p50 | >5,000 ms | <=1,000 ms |
| Ack latency p95 | >5,000 ms | >5,000 ms |
| Ack latency max | 32,144.378 ms | **19,017.787 ms** |
| Explicit force operations | One per nonzero-ack append | 131 |
| Forced data during produce | Not instrumented | 9,805.2 MiB |
| Time inside force operations | Not instrumented | 47,624.5 ms |
| Pending force data at measurement | Not instrumented | 57.2 MiB |
| Consumer verification | 10,000,000 passed | 10,000,000 passed |
| Consume throughput | 561,307 records/s | 473,058 records/s |
| Consume elapsed | 17.816 s | 21.139 s |
| Peak shared-JVM heap | 5,321.6 MiB | 5,221.3 MiB |

I start the acknowledgement timer when each asynchronous producer `send` is offered. This means the saturation latency also includes time spent waiting in the producer queue. The maximum improved, but p95 is still above five seconds because the producers can offer data faster than this disk can persist a sustained ten-gigabyte run.

## One-million-record calibration

The final periodic-flush calibration produced these results:

| Metric | Result |
| --- | ---: |
| Produce throughput | 614,413 records/s / 600.0 MiB/s |
| Produce elapsed | 1.628 s |
| Ack latency max | 449.623 ms |
| Force operations during produce | 8 |
| Consume throughput | 556,232 records/s / 543.2 MiB/s |
| Consumer verification | 1,000,000 / 1,000,000 passed |
| Peak shared-JVM heap | 1,259.0 MiB |

This short run gets a large benefit from the filesystem cache, so I don't use it as a sustained disk-throughput claim.

## What I changed in the flush path

The old Produce path used `force = acknowledgements != 0` for every partition append. This meant both `acks=1` and `acks=all` called `FileChannel.force(false)` synchronously while the partition lock was held.

I changed the design to:

1. Keep Kafka acknowledgement level separate from the local fsync policy.
2. Use periodic flushing by default, with one-second and 64 MiB thresholds.
3. Use one scheduled flusher for the broker instead of one thread for every partition.
4. Snapshot dirty generations while holding the partition lock, then call `force(false)` after releasing it.
5. Schedule an immediate background pass when the byte threshold is reached or a segment rolls over.
6. Force all remaining dirty segments during a clean shutdown.
7. Keep an explicit `sync` policy for strict per-append local persistence.
8. Recover an incomplete batch tail and remove later segments whose offsets depend on the damaged tail.

## What I changed in Fetch

The earlier load test also exposed a non-contiguous Fetch pagination bug. Fetch now stops at the first batch that does not fit after a response has started, while still preserving Kafka's first-batch size exception. I repeated both the one-million and ten-million tests after the fix, and both consumed every persisted record.

## Checks I ran

At the classic-group and static-replication milestone covered by this report, `sbt test` passed **25/25** tests:

- Wire codec unit tests.
- Fetch pagination regression tests.
- Periodic and synchronous flush-policy tests.
- Broker-wide background flush tests.
- Active and multi-segment crash-tail recovery tests.
- Raw-socket Kafka protocol integration tests, including proof that `acks=1` does not force inline.
- Durable committed-offset recovery and corrupt-tail truncation tests.
- Classic group-coordinator membership, synchronization, heartbeat, commit, and leave tests.
- Kafka 4.3.1 Admin, Producer, and Consumer end-to-end tests, including a two-consumer rebalance and committed-offset recovery after broker restart.
- Metadata-journal checksum recovery and uncommitted-replica visibility tests.
- A three-broker Kafka-client test for RF=3 replication, ISR shrink, leader-epoch promotion, continued production, and exact consumption after the original partition leader stops.

I then ran a one-million-record regression test. It passed exact verification at **607,661 produced records/s** and **544,883 consumed records/s**. This is within normal short-run variance of the 614,413 and 556,232 calibration, so I found no material idle group-coordinator regression in the data path.

After adding the cluster path, I ran the single-node one-million-record test again. It passed at **615,592 produced records/s** and **527,001 consumed records/s**. Produce acknowledgement latency stayed at or below 500 ms at p99, with a 422.632 ms maximum. The three-node path has correctness coverage, but I haven't run a dedicated sustained-throughput benchmark for it, so I don't present these numbers as replicated-cluster capacity.

After adding durable online partition reassignment, I ran the same one-million-record regression again. It consumed exactly **1,000,000 / 1,000,000** records at **540,738 produced records/s** and **283,351 consumed records/s**. The short-run consume result varied from the earlier cached calibration, but correctness and the production hot path remained healthy; reassignment recovery work is inactive when no move is running.

After adding dynamic voter membership and rollback for failed uncommitted `acks=all` attempts, I repeated the one-million-record regression on 2026-08-17. It consumed exactly **1,000,000 / 1,000,000** records at **367,202 produced records/s** and **250,135 consumed records/s**. Produce elapsed time was 2.723 seconds, consume elapsed time was 3.998 seconds, maximum acknowledgement latency was 1,265.882 ms, and peak heap was 1,311.1 MiB. The producer phase used 7.43 CPU cores in the shared JVM, so I record this as a correctness/regression pass rather than a new capacity result.

After adding quorum-replicated classic-group, offset, producer, and transaction state, I ran a clean **83/83** test suite and repeated the same one-million-record regression on 2026-08-18. It consumed exactly **1,000,000 / 1,000,000** records at **510,714 produced records/s** (**498.7 MiB/s**) and **263,637 consumed records/s** (**257.5 MiB/s**). Produce elapsed time was 1.958 seconds, consume elapsed time was 3.793 seconds, maximum acknowledgement latency was 536.512 ms, and peak heap was 1,335.7 MiB. The run forced 586.5 MiB in eight operations and left 399.7 MiB for the clean-shutdown force. Because the coordinator path is idle in this assigned-consumer harness, I use this as a hot-path regression and exactness check, not as a coordinator-capacity benchmark.

After adding deterministic fault injection, unclean-startup detection, force-kill recovery, torn-tail qualification, and stable/joint quorum partition tests, I ran a clean **99/99** suite and repeated the workload on 2026-08-20. It consumed exactly **1,000,000 / 1,000,000** records at **503,499 produced records/s** (**491.7 MiB/s**) and **250,190 consumed records/s** (**244.3 MiB/s**). Produce elapsed time was 1.986 seconds, consume elapsed time was 3.997 seconds, p50 acknowledgement latency stayed at or below 250 ms, p95/p99/p99.9 stayed at or below 500 ms, maximum acknowledgement latency was 445.075 ms, and peak heap was 1,222.1 MiB. The run stored 986.2 MiB, forced 597.0 MiB in eight operations, and left 389.2 MiB for clean shutdown. The result is consistent with the previous short regression; I use it as an exactness and hot-path guard, not as a dedicated-host or replicated-cluster capacity figure.

After adding time/size retention, conservative key compaction, offset expiry, timestamp/transaction indexes, bounded coordinator journals, atomic replacement recovery, and disk-pressure admission, I ran a clean **125/125** suite and repeated the workload on 2026-08-22. It consumed exactly **1,000,000 / 1,000,000** records at **423,662 produced records/s** (**413.7 MiB/s**) and **477,368 consumed records/s** (**466.2 MiB/s**). Produce elapsed time was 2.360 seconds, consume elapsed time was 2.095 seconds, p50 acknowledgement latency stayed at or below 500 ms, p95/p99/p99.9 stayed at or below 1,000 ms, maximum acknowledgement latency was 643.996 ms, and peak heap was 1,747.6 MiB. The run stored 986.2 MiB, forced 649.0 MiB in ten operations, and left 337.2 MiB for clean shutdown. The default lifecycle interval is five minutes, so cleanup did not run during this seven-second test. I record this as an exactness and inactive-lifecycle hot-path regression, not as cleanup throughput or production capacity.

After adding TLS/SASL, reloadable credentials and ACLs, audit logging, connection/request admission, per-principal quotas, and overload shedding, I ran a clean **149/149** suite and then added two focused policy-rotation/tool checks. I repeated the one-million workload twice on 2026-08-24. The first run produced **545,911 records/s** (**533.1 MiB/s**) and consumed **295,007 records/s** (**288.1 MiB/s**); the repeat produced **544,611 records/s** (**531.8 MiB/s**) and consumed **291,753 records/s** (**284.9 MiB/s**). Both consumed exactly **1,000,000 / 1,000,000** records. The repeat took 1.836 seconds to produce and 3.428 seconds to consume, p50 stayed at or below 500 ms, p95/p99/p99.9 stayed at or below 1,000 ms, maximum acknowledgement latency was 537.726 ms, and peak heap was 1,670.4 MiB. It forced 548.7 MiB in eight operations and left 437.6 MiB for clean shutdown. This harness uses the default plaintext listener with ACLs and quotas disabled, so I treat the result as a default-path exactness and hot-path guard, not as authenticated multi-tenant capacity.

After adding structured operations, Prometheus metrics, health/readiness, capacity alerts, Kafka Admin configuration reads, and atomic checksummed offline backup/restore, I ran the complete suite twice and finished with **180/180** passing tests. I repeated the same one-million workload on 2026-08-24. It consumed exactly **1,000,000 / 1,000,000** records at **533,937 produced records/s** (**521.4 MiB/s**) and **263,944 consumed records/s** (**257.8 MiB/s**). Produce took 1.873 seconds and consume took 3.789 seconds; p50 acknowledgement latency stayed at or below 250 ms, p95/p99/p99.9 stayed at or below 500 ms, and maximum acknowledgement latency was 415.569 ms. Peak heap was 1,395.1 MiB. The run stored 986.2 MiB, forced 559.0 MiB in eight operations over 398.7 cumulative milliseconds, and left 427.2 MiB for clean shutdown. The operations listener was disabled and no backup ran during the timed phases, so I use this result as an inactive-operations default-path regression guard, not as monitoring, backup, or replicated-cluster capacity.

After adding hostname-verified broker mTLS, certificate-bound node identities, live peer-policy rotation, security telemetry/readiness, and encrypted quorum-failover coverage, the clean suite exposed an interrupted-channel shutdown race. I hardened every durable journal close path and finished with **200/200** passing tests. I then reran the full ten-million workload on 2026-08-24. It consumed exactly **10,000,000 / 10,000,000** records at **350,319 produced records/s** (**342.1 MiB/s**) and **329,303 consumed records/s** (**321.6 MiB/s**). Produce took 28.545 seconds and consume took 30.367 seconds. Acknowledgement p50/p95/p99/p99.9 fell in the harness's `<=5000 ms` bucket and maximum latency was 3,636.325 ms. Produce used 4.12 CPU cores with 2,833 ms of GC; consume used 2.89 cores with 1,179 ms of GC. Peak heap was 5,336.7 MiB. The run stored 9,862.3 MiB, forced 9,674.0 MiB in 235 operations over 11,186 ms, and left 188.3 MiB pending for clean shutdown. The harness is single-node plaintext, so this qualifies exactness and the default data path; it does not measure peer TLS or replicated-cluster capacity.

## My conclusion

I fixed the bad flush behavior, the exact record checks pass, and sustained write performance improved a lot. On this development machine, explicit periodic persistence now tops out around 178 MiB/s.

If I want lower latency or more sustained throughput, the next options are faster or dedicated storage, multiple physical log devices, weaker flush guarantees, or replicated brokers using Kafka-style ISR durability.

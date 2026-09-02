# Coordinator delta qualification — 2026-09-02

I ran the coordinator milestone against source revision `8975fffa3b6eaf6a6d26aefcfb5958abbf66e038`. The final documentation commit does not change that tested implementation. I archived the [machine-readable report](2026-09-02-coordinator-scale.json).

## Environment and workload

- Windows, Java 21.0.11, Intel Core i5-12450HX, 8 cores/12 logical processors, about 21.7 GiB usable RAM, WD PC SN5000S 512 GB SSD, NTFS.
- Three in-process brokers with separate directories on the same local SSD; Kafka 4.3.1 clients; RF=3, minimum ISR=2, synchronous metadata durability.
- 1,000 group IDs, eight concurrent workers, two offset writes per group before controller loss, one afterward, and exact offset verification before failover, on the surviving majority, after the new writes, and after a full broker restart.
- The fault fixture uses 300 ms peer RPCs, 100 ms controller heartbeats, and a 600 ms election timeout. This is a qualification fixture, not the recommended production timing profile.
- Clients are created and closed per group per phase. This intentionally includes client churn; it is not a steady-state persistent-consumer benchmark or a membership-rebalance workload.

## Result

| Check | Result |
| --- | ---: |
| Full Scala/unit/integration/Kafka end-to-end/fault suite | 335/335 passed in 116 seconds |
| Acknowledged offset writes | 3,000 |
| Exact final offsets | 1,000/1,000 |
| Brokers serving coordinator writes | 1, 2, 3 |
| Controller failover and complete broker restart | Passed |
| Write phases, including client creation/closure | 98.507 seconds |
| Write throughput | 30.455 writes/s |
| Commit p50 / p95 / p99, including retries | 19.722 / 1,097.501 / 4,011.691 ms |
| Checkpoint attempts / rejected attempts | 3,036 / 36 |
| Proposed delta bytes / equivalent full-state bytes | 2,571,673 / 141,781,648 |
| Proposal-payload reduction | 98.2% |

The 36 rejected checkpoint attempts are retriable failures, not lost acknowledged offsets. Discovery/readiness rejections before checkpoint entry are not included in that counter. Full quorum replication and journal traffic are not reduced by the reported payload percentage.

## Interpretation

Correctness passed; I do not call the latency or throughput a production-capacity pass. During this campaign I observed 14,217 TIME_WAIT TCP sockets, while this host's configured IPv4 dynamic-port range had only 13,977 ports starting at 1024. Repeated client creation and reconnection therefore ran near the local socket-allocation limit. That is evidence of substantial environmental interference, not proof that all latency comes from the OS. The fixture's short timeouts and the remaining global quorum/image work also matter. I did not change the machine's networking configuration to make the test pass.

I need a dedicated-host comparison with recorded socket limits, long-lived clients, representative session/transaction churn, and independent shard persistence before making a horizontal-throughput claim. This milestone establishes atomic shard conflict isolation, smaller proposal payloads, bounded installation, failover/restart exactness, and repeatable measurement.

## Data-path regression

I also ran the existing one-million-record single-node plaintext harness with 1 KiB incompressible payloads, eight partitions, four producers, four consumers, LZ4, `acks=all`, and periodic flushing. It produced **482,015 records/s** in **2.075 seconds** and consumed **497,901 records/s** in **2.008 seconds**, verifying exactly **1,000,000/1,000,000**. Maximum acknowledgement latency was **769.943 ms**, storage was **986.2 MiB**, and peak heap was **3,212.3 MiB**. This remains a cache-assisted data-path regression, not a replicated coordinator or durability-capacity benchmark.

## Rolling regression

The pinned 1.0.0-to-current separate-process campaign passed all ten upgrade/rollback/activation/downgrade-rejection/recovery phases in **7.927 seconds**, with exact **40/40** records. It rebuilt the historical revision without modifications and exercised the new format-9 activation boundary. I retain the detailed operational boundary in the [rolling runbook](../rolling-upgrades.md).

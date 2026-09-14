# Delivery semantics qualification, 2026-09-14

I qualified this milestone on my Windows development machine with Eclipse Adoptium Java 21.0.11 and the repository's Kafka 4.3.1 client dependency. This is exactness and compatibility evidence, not a dedicated-host capacity result.

## What changed

- `InitProducerId` v0-v5 now supports expected producer ID/epoch retries without applying a second epoch bump.
- `AddPartitionsToTxn` v0-v5 supports flexible framing, batched transactions, and v4-v5 `verifyOnly` checks.
- `AddOffsetsToTxn`, `EndTxn`, and `TxnOffsetCommit` support v0-v4, including flexible v3-v4 layouts.
- Transactional offset commits validate modern consumer member identity and epoch before staging offsets.
- `DescribeProducers` v0, `DescribeTransactions` v0, and `ListTransactions` v0-v2 expose acknowledged delivery state through Kafka Admin.

I cap `EndTxn` and `TxnOffsetCommit` at v4 because v5 belongs to Kafka transaction protocol V2. Advertising v5 without its epoch-bump and dual-registration semantics would be a false compatibility claim. `InitProducerId` is capped at v5 because v6 is Kafka's unstable two-phase-commit extension.

## Automated transaction churn

I ran:

```powershell
.\sbt.bat "Test / runMain cascade.qualification.DeliverySemanticsQualification --transactions 256 --concurrency 16 --report artifacts/delivery-semantics-1.6.0.json"
```

The runner creates 256 independent transactional producers across 16 workers. Every producer commits one unique value and aborts another, then the runner verifies both isolation levels, restarts the broker from the same data directory, and repeats the exact committed-value comparison.

| Check | Result |
| --- | ---: |
| Transactional IDs | 256 |
| Concurrent workers | 16 |
| `read_committed` before restart | 256 / 256 exact |
| `read_uncommitted` before restart | 512 / 512 exact |
| `read_committed` after restart | 256 / 256 exact |
| Missing or unexpected values | 0 |
| Elapsed time | 11.183 seconds |

The machine-readable report returned `status=passed`. A small version of the same workload is part of the ordinary test suite.

## Whole-project regression

The complete Scala unit, integration, end-to-end, fault, security, and qualification run passed **564/564** tests with no failures or skips in **179 seconds**. Real Kafka 4.3.1 producers negotiated the expanded transaction RPC versions. Kafka Admin described active transactions and retained producer sequence state. Byte-level tests covered flexible headers, compact arrays, batched partition admission, `verifyOnly`, expected-epoch retries, filtering, error mapping, and stale consumer-epoch fencing.

## Boundary

This local run does not close the authenticated 72-hour multi-tenant soak, dedicated-host RF=3 capacity, broad Kafka client-version matrix, arbitrary packet-impairment campaign, or physical power/device-loss qualification. Those remain release gates in the production-readiness checklist.

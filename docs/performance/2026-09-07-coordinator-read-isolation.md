# Coordinator read-isolation qualification — 2026-09-07

I qualified immutable acknowledged coordinator reads on Windows 11 with Java 21.0.11 and 12 reported processors. The source campaign ran at revision `377b78cd4fc77e6817cc08381e44c3a6ab4103f8` on release metadata `1.3.1`. This is development-machine correctness and regression evidence, not a production capacity result.

## Result

The 1,000-group, 32-worker, persistent-client campaign passed:

| Check | Result |
| --- | ---: |
| Final offsets verified | 1,000 / 1,000 |
| Timed writes | 3,000 |
| Acknowledged offset snapshots | 3,000 |
| Offset keys returned | 3,000 |
| Controller failover | passed |
| Full three-broker restart | passed |
| Owners serving writes | 1, 2, 3 |
| Connection rejections | 0 |
| Offset-batch rejections | 0 |
| Publication rejections | 0 |
| Replication snapshot fallbacks | 0 |

Timed writes completed in 28.710 seconds at 104.495 writes/s. Commit latency was 95.682 ms p50, 1,099.673 ms p95, and 3,122.982 ms p99. The runner recorded 134 failed checkpoint attempts and 136 publication conflicts/failures while clients retried through ownership and controller transitions; exact verification and restart recovery still passed. I do not treat those percentiles as an SLO.

The offset workload intentionally produced zero stable-offset and transaction-visibility samples. Transaction read isolation is covered by the direct blocked-checkpoint tests and Kafka `read_committed` end-to-end tests, not by this group-offset campaign.

The full Scala regression passed 475/475 tests before the final evidence-only follow-up cases were added. A broader focused run passed 54/54 coordinator, broker, Kafka-client, network-fault, and read-isolation tests. The deterministic real-cluster test paused a non-controller owner's `CoordinatorDeltaCommit` RPC and proved that a wire-level `OffsetFetch` returned the old acknowledged offset without waiting for publication; it returned the new offset only after the RPC resumed and committed.

## Reproduce

```powershell
& 'C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot\bin\java.exe' `
  '-Dsbt.supershell=false' '-Dsbt.log.noformat=true' `
  -jar .tools\sbt-launch-1.12.6.jar `
  'Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --batch-max-requests 64 --batch-linger-ms 2 --publication-max-requests 64 --publication-linger-ms 2 --report artifacts/coordinator-read-isolation-1000.json'
```

The tracked [JSON report](2026-09-07-coordinator-read-isolation.json) is the exact successful result reformatted for review. The untracked artifact is still useful for local diagnostics, but I use the tracked copy as the durable evidence record.

## Interpretation

This result proves old-or-new acknowledged visibility under the covered publication failures and exact offset recovery at this cardinality. It does not prove a throughput improvement because I did not run a matched implementation-before/implementation-after comparison. The 2 ms publication settings also differ from older campaigns, so comparing their throughput would be misleading.

The shared metadata quorum, write-side group/delivery coordination, full image residency, transaction/membership churn, dedicated-host RF=3 capacity, multi-day soak, and real device-loss campaigns remain open production gates.

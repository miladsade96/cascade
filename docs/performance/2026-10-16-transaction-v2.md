# Kafka transaction protocol V2 qualification — 2026-10-16

I ran this qualification for Cascade 1.9.0 on Windows with Eclipse Adoptium Java 21.0.11. I used the Kafka 4.3.1 Java client already pinned by the build. This report describes a development-host correctness run; it is not a dedicated-host capacity, 72-hour soak, or physical-loss result.

## What I qualified

- Cluster-wide `transaction.version=2` capability exchange and unanimous finalized-feature activation.
- Produce v3-v12, including flexible Produce v9-v12 and implicit partition enrollment in v12.
- TxnOffsetCommit v0-v5, including implicit consumer-group enrollment in v5.
- EndTxn v0-v5, including atomic producer epoch advancement, retry-stable returned identities, and producer-ID rotation at epoch exhaustion.
- Durable protocol-version, active-transaction, completed-outcome, and resulting producer-identity state with headerless legacy-image decoding.
- Commit and abort visibility, transactional offsets, timeout fencing, producer reinitialization, and V1/V2 mutation-mixing rejection.
- Recovery after broker restart and transaction-coordinator failover.
- The append/publication race where replication completes after another coordinator mutation has installed a newer image.

## Commands and result

I first ran the focused codec, coordinator, wire, storage, and real-client suites while implementing the milestone. I then ran the release gate from a clean build:

```powershell
$env:JAVA_HOME='C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot'
$env:Path="$env:JAVA_HOME\bin;$env:Path"
.\sbt.bat clean test
```

The clean run passed **612/612** tests with **0 failures** and **0 errors** in **273 seconds**. The Kafka-client end-to-end suite passed 25/25 cases. The delivery coordinator suite passed 24/24, broker integration passed 25/25, delivery storage passed 9/9, coordinator-format coverage passed 7/7, and binary codec coverage passed 8/8.

The first complete run found three snapshot-size accounting assertions that still assumed the previous delivery-image header size. I replaced that fixed byte count with the encoded empty-image size, reran the two affected suites at 5/5, and then ran the clean 612-test gate above. I record the discovery because the release result should explain defects found during qualification instead of hiding them.

## Boundary

I do not call this a complete Kafka replacement based on this run. I still require a broader producer and client-version matrix, high-cardinality transactional churn on dedicated RF=3 hosts, the authenticated 72-hour multi-tenant soak, arbitrary packet impairment, coordinated multi-host restore drills, and physical power/device-loss qualification. `InitProducerId` v6 also remains intentionally unadvertised because its unstable two-phase-commit extension is outside this milestone.

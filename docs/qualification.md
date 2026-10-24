# Soak and physical-loss qualification

I use these runners to produce auditable evidence rather than treating a short unit test as a production qualification.

## Portfolio failover and comparison evidence

I compile the test tools once, then run the exact 1.16.0 image through the short leader-failover demo:

```powershell
./sbt.bat Test/compile
./scripts/run-failover-demo.ps1 -Image miladsade96/cascade:1.16.0 -Records 100000 -Java <Java-21-executable>
```

For the controlled RF=1 comparison, I run the same Kafka Java workload against Cascade and the official Kafka image:

```powershell
./scripts/compare-kafka.ps1 -CascadeImage miladsade96/cascade:1.16.0 -KafkaImage apache/kafka:4.3.1 -Records 250000 -Java <Java-21-executable>
```

The [failover guide](failover-demo.md) defines the failure timestamp and exactness gate. The [benchmark guide](benchmarking.md) defines the workload, environment inventory, and publication rules.

## Consumer protocol

For the Kafka 4.3.1 consumer-protocol milestone I run the wire, coordinator, real-client, and acknowledged-read gates together:

```powershell
.\sbt.bat "testOnly cascade.protocol.CompatibilityContractSuite cascade.group.GroupStateSuite cascade.group.OffsetStoreSuite cascade.group.GroupCoordinatorSuite cascade.broker.BrokerIntegrationSuite cascade.e2e.KafkaClientEndToEndSuite cascade.cluster.CoordinatorReadIsolationQuorumSuite"
```

The final release gate is still `.\sbt.bat test`. The 2026-09-14 run passed 564/564 tests in 179 seconds. That result is correctness and local regression evidence, not a multi-day churn or dedicated-host capacity result.

## Delivery semantics

I run the complete wire/coordinator/client regression and a configurable exact transaction churn campaign:

```powershell
.\sbt.bat "testOnly cascade.protocol.CompatibilityContractSuite cascade.delivery.DeliveryCoordinatorSuite cascade.delivery.DeliveryReadViewSuite cascade.group.GroupCoordinatorSuite cascade.broker.BrokerIntegrationSuite cascade.e2e.KafkaClientEndToEndSuite cascade.qualification.DeliverySemanticsQualificationSuite"
.\sbt.bat "Test / runMain cascade.qualification.DeliverySemanticsQualification --transactions 256 --concurrency 16 --report artifacts/delivery-semantics-1.6.0.json"
```

I require exact committed and uncommitted value sets, no duplicates or omissions, and the same committed set after broker restart. The 2026-09-14 development-host run passed with 256 transactional IDs, 16 concurrent workers, 256 committed records, 512 uncommitted records, 256 recovered committed records, and zero mismatches. The [dated report](performance/2026-09-14-delivery-semantics.md) keeps the scope and remaining limits.

For transaction protocol V2 I also require feature negotiation, flexible Produce v12, implicit partition and group enrollment, retry-stable EndTxn v5 identities, epoch rollover, protocol-mixing rejection, restart recovery, and coordinator failover:

```powershell
.\sbt.bat "testOnly cascade.protocol.BinaryCodecSuite cascade.protocol.CompatibilityContractSuite cascade.coordinator.CoordinatorFormatSuite cascade.delivery.DeliveryStoreSuite cascade.delivery.DeliveryCoordinatorSuite cascade.broker.BrokerIntegrationSuite cascade.e2e.KafkaClientEndToEndSuite"
.\sbt.bat clean test
```

On 2026-10-24 the 1.16.0 release tree passed 692/692 tests in 206 seconds with no failures or errors. The run includes the authenticated RF=3 soak smoke, immutable adjacent-image inspection, hardened Docker lifecycle tests, exact cross-version traffic accounting, legacy producer-checkpoint duplicate recovery, and the client-version matrix in addition to the coordinated snapshot, replicated cleanup, producer-state, transaction V2, magic-v2, and all-codec compaction gates. The [1.16.0 release notes](releases/1.16.0.md) record the current result; the required 72-hour campaign remains separate and open.

## Coordinated snapshots

I run the manifest, request-barrier, internal-wire, participant-fencing, three-broker, repository, command, coordinator-format, quorum-codec, and recovery suites together:

```powershell
.\sbt.bat "testOnly cascade.backup.* cascade.coordinator.CoordinatorFormatSuite cascade.coordinator.CoordinatorQuorumRequestCodecSuite cascade.e2e.BackupRecoveryEndToEndSuite cascade.group.OffsetBatchQuorumSuite"
```

The gate requires one artifact per voter and durable directory ID, an unchanged controller term and metadata fingerprint, equal high watermarks on every ISR, atomic global-manifest publication, barrier recovery after lost releases, and no publication after dropped prepare or capture calls. Verification must reject missing, extra, corrupt, unsafe, or mismatched artifacts. Retention must keep incomplete directories, fail closed before deleting anything if a published generation is invalid, and delete only verified generations with enough newer verified replacements. The focused 2026-10-21 run passed 45/45 tests; the whole-software gate remains mandatory.

## Adjacent release images and Java client versions

I qualify the real 1.15.0-to-1.16.0 image boundary only after both immutable images exist locally:

```powershell
.\scripts\qualify-adjacent-images.ps1 `
  -BaselineImage miladsade96/cascade:1.15.0 `
  -BaselineVersion 1.15.0 `
  -CandidateImage miladsade96/cascade:1.16.0 `
  -CandidateVersion 1.16.0 `
  -Report artifacts/adjacent-images-1.15.0-to-1.16.0.json
```

The pass condition includes immutable identity and release-label checks, pre-upgrade rollback, idempotent traffic through abrupt baseline-leader replacement, at least one measured Kafka producer retry, exact contiguous acknowledgements, the declared post-upgrade downgrade behavior, all-candidate restart recovery, exact records, exact committed offset, and no broker protocol errors. Source-only rolling evidence cannot satisfy this gate.

The 2026-10-21 campaign passed all ten phases in 96.321 seconds. It preserved 1,200 exact records and committed offset 1,200 while recording 100 Kafka producer retries, replacing baseline leader 1, rolling node 2 back before full upgrade, rejoining 1.14.0 after all nodes had run 1.15.0, and recovering from an all-candidate restart. The [dated report](performance/2026-10-21-adjacent-image-rolling.md) records the image identities, defects found in the qualification harness, and boundaries.

I then run five independently resolved Apache Kafka Java client releases against the exact candidate image:

```powershell
.\scripts\qualify-java-client-versions.ps1 `
  -BrokerImage miladsade96/cascade:1.16.0 `
  -ExpectedVersion 1.16.0 `
  -Report artifacts/java-client-versions-1.16.0.json
```

The reviewed matrix is 3.9.1, 4.0.0, 4.1.1, 4.2.0, and 4.3.1. Every client must independently create a topic, produce 25 idempotent records with exact offsets, consume the exact ordered values through a group, commit offset 25, and retrieve that offset through Admin. The report is bound to the candidate image ID and Git revision. The existing five-language matrix remains a separate gate because one Java version matrix does not cover KafkaJS, librdkafka, franz-go, or Confluent.Kafka .NET.

## Distributed quotas

I run the local bucket, peer-codec, exact ledger, metric, rolling-activation, and three-broker fault tests together:

```powershell
.\sbt.bat "testOnly cascade.security.RequestQuotaSuite cascade.security.DistributedQuotaCodecSuite cascade.security.ClusterQuotaLedgerSuite cascade.operations.PrometheusMetricsSuite cascade.coordinator.CoordinatorFormatSuite cascade.fault.DistributedQuotaFaultSuite"
```

The cluster gate fails unless one broker can consume capacity that the two idle brokers did not use, concurrent reservations stay within the one global burst, inconsistent limits fail closed, and a replacement controller cannot reuse its predecessor's burst. I still finish with `.\sbt.bat test`; short deterministic tests do not replace the authenticated 72-hour multi-tenant soak or dedicated-host quota capacity work.

## Coordinator read isolation

I run the deterministic blocked-publication and real three-broker pause tests before a coordinator release:

```powershell
.\sbt.bat "testOnly cascade.group.OffsetReadViewSuite cascade.group.OffsetCommitIsolationSuite cascade.delivery.DeliveryReadViewSuite cascade.delivery.DeliveryCoordinatorSuite cascade.coordinator.CoordinatorReadMetricsSuite cascade.fault.NetworkFaultControllerSuite cascade.cluster.CoordinatorReadIsolationQuorumSuite"
```

The real-cluster test pauses one specific owner-to-follower shard-finalize call. It fails if `OffsetFetch` waits for that paused write, returns the tentative value, or fails to return the new value after finalization resumes. The fault controller has bounded waits and fails closed if the expected RPC never arrives.

I then run the cardinality/recovery campaign with explicit batching and publication settings:

```powershell
.\sbt.bat "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --batch-max-requests 64 --batch-linger-ms 2 --publication-max-requests 64 --publication-linger-ms 2 --report artifacts/coordinator-read-isolation-1000.json"
```

I require `status=passed`, 1,000/1,000 verified groups, 3,000 timed writes, controller failover, full restart recovery, all three owners, no connection/batch/publication admission rejection, and at least 2,000 acknowledged offset snapshots and keys. I archive the raw JSON, source revision, Java version, processor count, and command. This workload does not exercise transactional Fetch, so zero stable-offset or transaction-visibility counters are expected here; their blocked-checkpoint and Kafka-client suites remain mandatory.

The original acknowledged-read result is recorded in the [2026-09-07 report](performance/2026-09-07-coordinator-read-isolation.md). The current format-12 command and result are in the [coordinator architecture runbook](coordinator-architecture.md). I do not compare throughput across different architectures as a controlled benchmark or use development-host output as dedicated-host capacity evidence.

## Multi-day soak

The authenticated soak profile starts an RF=3 cluster with synchronous flushing, peer mTLS, per-tenant `SASL_SSL` identities, deny-by-default ACLs, distributed quotas, committed and aborted transactions, scheduled consumer rebalances, controller churn, live TLS rotation, and final full-cluster restart recovery. It compares every acknowledged offset and payload before and after restart and writes an atomic JSON report.

```powershell
$env:JAVA_HOME='C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
.\scripts\run-authenticated-soak.ps1 `
  -EvidenceDirectory "D:\cascade-evidence\soak-1.16.0-$(Get-Date -Format yyyyMMdd-HHmmss)" `
  -DurationHours 72 `
  -Tenants 8 `
  -RecordsPerCycle 100 `
  -PayloadBytes 512
```

The wrapper records the exact clean revision, Java runtime, OS, architecture, processor count, workload and fault intervals. It fails closed on missing evidence, any record/offset mismatch, incomplete restart recovery, missing tenant audit evidence, authentication failure, quota rejection, or a scheduled rebalance, TLS rotation, or controller failover that did not happen. The complete procedure and JSON acceptance fields are in the [authenticated soak runbook](authenticated-soak.md).

The automated suite forces every phase through a one-second two-tenant smoke campaign. That proves the harness and its failure checks, not 72 hours of stability. I do not mark the production gate complete until a clean release candidate runs the full duration on the documented qualification host and I archive its untouched evidence directory.

## Physical power or device loss

I run the probe against an external three-broker deployment. Its write phase forces independent witness evidence for each acknowledged record. I put that evidence path on a control machine or device that will not lose power with the brokers.

```powershell
.\sbt.bat "Test / runMain cascade.qualification.PowerLossProbe write --bootstrap 10.0.0.11:9092,10.0.0.12:9092,10.0.0.13:9092 --topic cascade-power-loss --evidence Z:\cascade-witness\campaign-01.log"
```

While the write phase is active, I cut power with an external PDU or remove the selected storage device. I do not stop the JVM or operating system cleanly. After hardware recovery and quorum readiness, I run:

```powershell
.\sbt.bat "Test / runMain cascade.qualification.PowerLossProbe verify --bootstrap 10.0.0.11:9092,10.0.0.12:9092,10.0.0.13:9092 --topic cascade-power-loss --evidence Z:\cascade-witness\campaign-01.log"
```

The verifier consumes every witnessed offset and compares its checksum. I repeat the campaign for leader-host loss, follower-host loss, controller loss, whole-device removal, power loss during flush, power loss during compaction, and power loss during replica recovery. I archive SMART/NVMe health, filesystem diagnostics, controller/drive cache policy, PDU timestamps, broker logs, and verifier output.

The runner is implemented, but I will not claim physical power/device-loss qualification until these campaigns pass on the documented target hardware.

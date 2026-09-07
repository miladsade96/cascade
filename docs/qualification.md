# Soak and physical-loss qualification

I use these runners to produce auditable evidence rather than treating a short unit test as a production qualification.

## Coordinator read isolation

I run the deterministic blocked-publication and real three-broker pause tests before a coordinator release:

```powershell
.\sbt.bat "testOnly cascade.group.OffsetReadViewSuite cascade.group.OffsetCommitIsolationSuite cascade.delivery.DeliveryReadViewSuite cascade.delivery.DeliveryCoordinatorSuite cascade.coordinator.CoordinatorReadMetricsSuite cascade.fault.NetworkFaultControllerSuite cascade.cluster.CoordinatorReadIsolationQuorumSuite"
```

The real-cluster test pauses one specific owner-to-controller coordinator commit. It fails if `OffsetFetch` waits for that paused write, returns the tentative value, or fails to return the new value after publication resumes. The fault controller has bounded waits and fails closed if the expected RPC never arrives.

I then run the cardinality/recovery campaign with explicit batching and publication settings:

```powershell
.\sbt.bat "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --batch-max-requests 64 --batch-linger-ms 2 --publication-max-requests 64 --publication-linger-ms 2 --report artifacts/coordinator-read-isolation-1000.json"
```

I require `status=passed`, 1,000/1,000 verified groups, 3,000 timed writes, controller failover, full restart recovery, all three owners, no connection/batch/publication admission rejection, and at least 2,000 acknowledged offset snapshots and keys. I archive the raw JSON, source revision, Java version, processor count, and command. This workload does not exercise transactional Fetch, so zero stable-offset or transaction-visibility counters are expected here; their blocked-checkpoint and Kafka-client suites remain mandatory.

The current development-host result is recorded in the [2026-09-07 report](performance/2026-09-07-coordinator-read-isolation.md). I do not compare throughput across different batching/publication settings or use it as dedicated-host capacity evidence.

## Multi-day soak

The soak runner starts an isolated Cascade broker, creates one topic per tenant, continuously produces deterministic payloads, consumes exact offsets and bytes, samples heap use, and writes an atomic JSON report. The default duration is 72 hours.

```powershell
$env:JAVA_HOME='C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot'
$env:PATH="$env:JAVA_HOME\bin;$env:PATH"
.\sbt.bat "Test / runMain cascade.qualification.SoakTest --duration-hours 72 --tenants 8 --records-per-cycle 100 --payload-bytes 512 --report artifacts/soak-72h.json --keep-data"
```

I archive the report, broker events, metrics, JVM/OS telemetry, hardware description, exact commit, and retained data directory. I treat any missing/duplicate/corrupt record, unexpected broker exit, validation error, sustained backlog, or unexplained heap trend as a failure. The automated suite runs a one-second smoke check of this harness; it does not substitute for 72 hours of elapsed time.

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

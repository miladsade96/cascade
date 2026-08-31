# Soak and physical-loss qualification

I use these runners to produce auditable evidence rather than treating a short unit test as a production qualification.

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

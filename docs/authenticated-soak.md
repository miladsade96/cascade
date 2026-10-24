# Authenticated multi-tenant soak

I use this campaign to keep long-running correctness separate from short unit and integration tests. It is a release gate, not a throughput benchmark.

## What the runner exercises

The `authenticated-cluster` profile starts three Cascade brokers with replication factor three, minimum ISR two, and synchronous flushing. Every broker has a separate durable directory and certificate. Broker traffic uses hostname-verified mutual TLS. Kafka clients use `SASL_SSL` with a different PLAIN identity for every tenant, deny-by-default ACLs, and controller-fenced distributed quotas.

Every tenant owns one topic, transaction ID, producer, and exact verifier. Each cycle commits a transaction and periodically aborts a separate transaction. The verifier uses `read_committed` and compares every returned offset and payload with the acknowledged producer metadata; aborted values must never enter that ledger.

The schedule uses elapsed time instead of completed cycle count, so a slower broker cannot silently reduce fault frequency:

- consumer-group rebalance every five minutes;
- active-controller stop, election, and restart every fifteen minutes;
- live CA overlap, peer leaf replacement, and final trust cutover after one hour;
- committed and aborted transactions throughout the run; and
- a complete three-broker restart followed by a second exact read from offset zero.

The final report also requires every configured tenant principal to appear in forced audit output, at least one distributed quota reservation, zero authentication failures, zero quota rejection, and exact equality between acknowledged, first-pass verified, and restart-recovered records.

## Release command

I run this on a dedicated qualification machine with a clean release-candidate tree and JDK 21:

```powershell
$env:JAVA_HOME = 'C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot'
$env:PATH = "$env:JAVA_HOME\bin;$env:PATH"

.\scripts\run-authenticated-soak.ps1 `
  -EvidenceDirectory "D:\cascade-evidence\soak-1.16.0-$(Get-Date -Format yyyyMMdd-HHmmss)" `
  -DurationHours 72 `
  -Tenants 8 `
  -RecordsPerCycle 100 `
  -PayloadBytes 512
```

The wrapper refuses an existing evidence directory, dirty source by default, a non-JDK-21 runtime, a missing report, or any failed acceptance field. It writes `environment.json`, `runner.log`, and `result.json`. `result.json` is atomically replaced during progress updates, so an observer never reads a partially written document. I use `-KeepData` only when I have enough storage to retain every broker directory; failed Scala campaigns preserve their temporary data path in the captured log for diagnosis.

`-DurationSeconds` exists for smoke qualification. It does not turn a short run into multi-day evidence. `-AllowDirtyTree` is for harness development only and makes the recorded revision include `+working-tree`.

## Acceptance

I accept a release campaign only when all of these are true:

- the configured duration is 259,200 seconds and the process exits normally;
- `status` is `passed`, `failure` is null, and `mismatches` is zero;
- `records`, `verified_records`, and `recovered_records` are equal and non-zero;
- every tenant is present in authentication audit evidence;
- authentication failures and quota rejections are zero;
- quota reservations, rebalances, TLS rotations, broker restarts, and controller failovers are non-zero;
- `restart_verified` is true; and
- the environment record identifies the exact clean Git revision, Java runtime, OS, architecture, processor count, and workload settings.

Expected retriable connection errors can appear while the active controller is deliberately absent. They do not waive any acceptance field: producer transactions, exact reads, quota coordination, and authentication must recover within their bounded client timeouts.

## Failure handling

I do not edit a failed report into a pass or reuse its directory. I keep its log and data when possible, record the first failing phase, and start a fresh evidence directory after fixing the cause. A missing final report, host reboot, out-of-space condition, hung client, TLS reload failure, denied tenant, quota rejection, missing scheduled event, offset mismatch, duplicate, unexpected value, or incomplete restart verification fails the campaign.

This harness deliberately keeps all three brokers inside one qualification JVM. It exercises the real broker, network listener, TLS/SASL/ACL, Kafka clients, durable directories, elections, replication, transactions, quotas, and restart recovery, but it is not evidence for independent host failure domains, a physical power cut, device removal, arbitrary external packet impairment, or a dedicated-host capacity envelope. I keep those as separate production gates.

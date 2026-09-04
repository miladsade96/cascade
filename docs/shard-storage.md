# Shard-isolated coordinator storage

I store changed coordinator shard payloads separately from the ordered metadata commit journal. This is a staged capacity change: immutable, independently checksummed shard objects and small atomic commit references first; independent shard consensus and service locks remain separate work.

The feature is included in `1.3.0`, following qualification during 1.3.0-SNAPSHOT development. It is not part of the older 1.2.0 image. I keep image status and upgrade limitations in the [release notes](releases/1.3.0.md).

## Safety contract

- Existing clusters keep their inline journal until every voter supports the new feature and format floor.
- I force every referenced object before forcing its commit marker. An interrupted preparation is an orphan, not a committed update.
- One marker publishes every shard of a transaction together, with the existing exact-base validation and quorum rules.
- Recovery must reject a missing, truncated, or corrupt referenced object without truncating a valid commit marker. It may ignore objects that were never published.
- Full snapshots at migration, topology changes, and checkpoints preserve byte-exact coordinator images. Changed shards use separate objects between these checkpoints.
- Reclamation occurs only after a forced, atomically published self-contained checkpoint. It must never delete an object required by any retained journal frame.
- Backup and restore must include the journal and its object directory as one artifact under the existing write barrier.
- Counters distinguish marker bytes, newly forced object bytes, reuse, and reclamation. Smaller markers do not imply smaller total I/O or independent consensus.

## Qualification plan

I require object/record codec and size-bound tests, concurrent preparation, multi-shard atomicity, torn markers, orphan preparation, missing/corrupt object rejection, checkpoint/reclamation restart tests, feature negotiation, cluster failover, exact backup/restore, and the complete regression suite. The long-lived Kafka-client runner separates connection churn from coordinator work, reports latency and actual disk/wire counters, and verifies offsets through failover and restart. The existing churn benchmark remains a regression workload. I archive the [measured qualification](performance/2026-09-02-shard-storage.md), including failed attempts and remaining limits.

This work does not qualify multi-day soak, physical power/device loss, independent per-shard consensus, or dedicated-host production capacity. The force/rename contract must still be qualified on the target filesystem and devices.

## Layout and recovery

`shard-object-storage` level 1 requires metadata format 11 plus the active incremental-coordinator features. Before activation, format-10 peers continue using inline journal payloads. After activation, the journal uses signed discriminator `-11`: kind 0 is a full checkpoint descriptor and kind 1 is one atomic changed-shard reference set. Wire replication remains the existing inline delta/snapshot protocol; objects are local to each replica, not fetched from another node on demand.

The object directory is `<metadata-journal-name>.shards` beside the journal. Names bind a fixed shard namespace to a SHA-256 content digest. Ordinary changed-shard payloads use namespaces 0–128; full group/delivery snapshots use 129 and 130. Full checkpoints retain the original image bytes, including legacy versions and ordering. This is shard-isolated payload persistence between full checkpoints—not separate per-shard append journals or independent consensus.

Preparation writes a temporary file, forces it, publishes its immutable name, and forces the directory where supported. Only then can the forced journal marker publish the update. A failed write or checkpoint fences subsequent metadata writes until restart/recovery, including errors after the marker may already be durable. Missing referenced data fails startup and preserves the journal for investigation; copying just the journal is no longer a backup.

Heartbeat reconciliation shares the metadata publication lock. A follower can durably publish a candidate before the leader receives its acknowledgement and publishes locally; that transient position must not be mistaken for a stale controller. Higher controller terms still fence immediately. I test the overlap by holding completed follower commit replies while a heartbeat observes the candidate, and also inject a real broker checkpoint failure to verify service fencing.

The compaction budget includes marker bytes and new object bytes since the previous checkpoint. After forcing and publishing a self-contained checkpoint, Linux directory forcing permits reclamation of obsolete recognized objects. On the tested Windows/JDK combination directory forcing is unavailable, so automatic reclamation deliberately retains old objects. The replay budget still resets at each checkpoint, but retained disk history can grow: I monitor it and do not claim bounded Windows disk usage or Windows power-loss qualification. I never manually delete objects from a running store. Unknown files are not garbage-collected.

## Metrics

All object metrics have only the `node_id` label:

- `cascade_coordinator_object_bytes_written_total` and `cascade_coordinator_objects_written_total`: newly forced payloads, including checkpoint objects.
- `cascade_coordinator_objects_reused_total`: existing payloads revalidated before reuse.
- `cascade_coordinator_object_bytes_reclaimed_total` and `cascade_coordinator_objects_reclaimed_total`: post-checkpoint reclamation.
- `cascade_coordinator_object_bytes`: observed stored bytes, including retained history and orphans.
- `cascade_coordinator_directory_force_supported`: whether this store can force directory publication.

Existing metadata journal byte counters now measure reference markers on the activated path. I add object bytes to marker/checkpoint bytes before discussing total persistence I/O; these counters exclude filesystem allocation/metadata overhead. Process-lifetime counters reset after restart. Immutable image-local shard caches reduce repeated decoding, but full-state serialization, validation, and shared locking still exist.

## Reproduce

```text
./sbt test stage
./sbt "Test/runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 8 --rounds 2 --client-lifecycle persistent --report artifacts/shard-persistent.json"
./sbt "Test/runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 8 --rounds 2 --client-lifecycle churn --report artifacts/shard-churn.json"
```

Persistent mode creates one client per group, warms it with one offset commit, and reuses that client through verification, controller loss, further writes, and complete broker restart. Warmup time/writes are reported separately from timed writes; disk/wire counters include warmup and stop before final restart. Client residency is capped at 2,000 to bound this development harness. Churn mode retains the original create/close-per-phase workload. Neither mode is a membership/transaction churn benchmark or a three-host production-capacity claim.

All clients in this harness share one IP. Persistent mode explicitly provisions `max(1000, 3 * groups + 32)` connections per IP for bootstrap, metadata, coordinator, and peer traffic; broker defaults remain unchanged. The JSON records this budget and admission rejections, and qualification rejects any connection-admission loss before or after restart. Increasing a client's API timeout is not a substitute for provisioning its connections. Phase and failure diagnostics identify where a failed campaign stopped.

```powershell
./scripts/qualify-rolling-upgrade.ps1 -Baseline format10 -Report artifacts/shard-rolling-format10.json
./scripts/qualify-shard-storage-linux.ps1
```

The Linux script uses already-compiled classes and pinned JDK dependencies in disposable read-only containers. It requires directory forcing and actual reclamation to pass, then runs the storage/backup regression suites on Linux `/tmp` backed by tmpfs. The probe is code-path qualification, not physical power-loss evidence or a target-device benchmark. For profiling I run the persistent benchmark with Java Flight Recorder enabled and keep the recording alongside the JSON report. I retain the existing format-9 and 1.0.0 rolling gates as regressions.

# Backup and restore runbook

I use Cascade's maintenance commands for offline, file-exact disaster-recovery copies, a local online snapshot, and a controller-coordinated all-voter snapshot. I treat a generation as cluster-authoritative only when its global manifest and every referenced node artifact verify together.

## What the backup contains

The tool copies every regular file under `--data-dir`, including topic segments, indexes, high-watermark checkpoints, legacy coordinator journals, `.cascade/coordinator-quorum/shard-NNN.log` files, cluster metadata, and the clean-shutdown marker. It does not follow symbolic links and refuses an unsupported file type. Restoring only cluster metadata after format 12 activation loses independently committed coordinator state, so I always preserve and restore the whole broker data directory.

My broker command-line configuration, TLS key stores, credential/ACL files, operations token, audit log, and structured event log are included only if I deliberately placed them under the data directory. I normally keep those secrets and configuration in a separate encrypted configuration backup. I record the matching node ID, listener addresses, cluster bootstrap settings, software revision, and JDK alongside each backup.

The generated `cascade-backup.manifest` contains a creation timestamp, exact relative file list, length, and SHA-256 checksum for each source file. Cascade copies into a private staging directory beside the requested target, forces copied files and the manifest, checks that the stopped source did not change, and atomically renames the staging directory into place. The target must not already exist and must be outside the source.

## Create and verify a backup

For a single broker I:

1. Stop producers or route them elsewhere.
2. Stop Cascade normally and wait for `broker_stopped`.
3. Confirm that no broker process is using the data directory.
4. Create the backup into a new destination.
5. Verify it before and after copying it to off-host storage.

On Windows:

```powershell
.\sbt.bat "run backup --data-dir D:\cascade\data-1 --backup-dir E:\cascade-backups\node-1-2026-08-24T1815Z"
.\sbt.bat "run verify-backup --backup-dir E:\cascade-backups\node-1-2026-08-24T1815Z"
```

On Linux or macOS:

```bash
./sbt "run backup --data-dir /var/lib/cascade/node-1 --backup-dir /srv/cascade-backups/node-1-2026-08-24T1815Z"
./sbt "run verify-backup --backup-dir /srv/cascade-backups/node-1-2026-08-24T1815Z"
```

The command refuses a live or unclean source. After a forced process or host failure I first start Cascade against that directory so it can perform conservative recovery, verify the data through a Kafka client, and stop it cleanly. Only then do I create the backup.

For an offline cluster-consistent disaster-recovery set I still quiesce writes and stop every broker cleanly before backing up every node. A rolling per-node backup can be useful for local recovery, but it may contain different committed moments and I do not treat it as an atomic cluster snapshot. The coordinated online path below is the supported alternative when I cannot stop the cluster.

## Create an online broker snapshot

The embedded broker API exposes `createOnlineSnapshot(target)`. It takes the broker's exclusive snapshot barrier, waits for admitted requests to finish, pauses lifecycle and background-flush workers, forces every local partition and coordinator file, publishes the checksummed snapshot, and then resumes traffic. The automated test proves the artifact contains every record acknowledged before the barrier and none acknowledged after it. This API remains useful for one broker, but its artifact is not a cluster snapshot.

## Create a coordinated cluster snapshot

I call `createClusterSnapshot(target)` on the active controller. The feature activates only after every voter supports `cluster-snapshot=1`. The controller refuses a joint membership, an unavailable voter, or a stale controller. It then freezes every voter behind a leased request barrier, captures all node artifacts concurrently, and releases every barrier even if preparation or capture fails.

The snapshot identity binds the controller ID and term, metadata version, and metadata SHA-256. Every node rechecks that identity before and after copying, binds its artifact to its durable directory ID, and reports exact per-partition high watermarks. The controller requires one artifact per voter, matching directory identities, equal high watermarks across each ISR, and an unchanged cluster position. Only then does it atomically publish `cascade-cluster-snapshot.manifest`. A dropped prepare or capture leaves no global manifest, so a partial directory is not an authoritative generation. Lease expiry recovers a node if a release message is lost.

The `target` path is interpreted locally by every broker. In a shared backup filesystem, I mount the same absolute generation path on every host and give each node write access only to its `node-N` directory. With host-local staging, I reserve the same unused path on every host, let the controller publish the global manifest on its host, and then collect each complete `node-N` directory beneath that manifest without renaming it. I verify the collected generation before transfer, after transfer, and before restore. The expected layout is:

```text
generation-20261021T071500Z/
  cascade-cluster-snapshot.manifest
  node-1/cascade-backup.manifest
  node-2/cascade-backup.manifest
  node-3/cascade-backup.manifest
```

I verify a collected generation with:

```powershell
.\sbt.bat "run verify-cluster-snapshot --snapshot-dir E:\cascade-snapshots\generation-20261021T071500Z"
```

Verification rejects missing or extra top-level entries, symlinks, unsafe paths, node/directory identity changes, local manifest mismatches, file-count or byte-count changes, and any local backup checksum failure.

## Protect and retain backups

The built-in format provides integrity, not confidentiality. I encrypt backup storage, restrict it to the recovery identity, keep at least one copy off the broker host and failure domain, and use immutable/object-lock retention where available. I never edit a published backup in place.

Scheduling and encrypted transfer remain operator-owned. My job scheduler creates a new generation, checks the snapshot result, collects every artifact, runs `verify-cluster-snapshot`, transfers the complete generation, and verifies it again. I can then retain the newest seven verified generations with:

```powershell
.\sbt.bat "run prune-cluster-snapshots --repository-dir E:\cascade-snapshots --retain 7"
```

Pruning validates every published generation before deleting anything and deletes only a verified generation that has the requested number of newer verified replacements. An incomplete directory is left untouched. A corrupt published generation fails the entire operation closed, so I investigate rather than allowing retention to erase evidence.

## Restore

I restore only into a path that does not exist:

```powershell
.\sbt.bat "run verify-backup --backup-dir E:\cascade-backups\node-1-2026-08-24T1815Z"
.\sbt.bat "run restore --backup-dir E:\cascade-backups\node-1-2026-08-24T1815Z --data-dir D:\cascade\restored-node-1"
```

Restore verifies the manifest and exact file set before copying. It verifies length and SHA-256 again for every staged file, confirms that the source backup did not change, forces the restored files, and atomically publishes the destination. It does not copy the backup manifest into the broker data directory.

Before startup I restore the matching external configuration and secrets, verify ownership/permissions, confirm the node ID and listener addresses, and make sure the original broker with that node identity cannot start at the same time. I start Cascade against the restored directory, expect `recovery=Clean`, wait for readiness, and verify topic metadata, end offsets, representative records, consumer offsets, and transaction visibility through a Kafka client.

For a full-cluster restore I first run `verify-cluster-snapshot`, then restore each `node-N` artifact with the normal `restore` command into the data directory for that exact node and durable directory identity. I keep node identities and external configuration paired, isolate the restored cluster from the original network, restore every voter, start the initial voters, wait for controller/readiness convergence, start remaining nodes, and run exact application-level record, offset, and transaction verification before admitting producers. I never combine node directories from different snapshot generations.

## Failure interpretation

- `backup requires a cleanly stopped broker` means I must recover and cleanly stop the source; I do not remove or manufacture marker files.
- `backup source changed` or `broker started while the backup was running` means the copy is discarded and I repeat the procedure after stopping the broker.
- `contents do not match the manifest`, `length mismatch`, or `checksum mismatch` means the backup is corrupt or was modified. I quarantine it and use another verified generation.
- `target already exists` is a safety boundary. I choose a new empty path rather than asking the tool to overwrite data.
- An atomic-move failure leaves no published target; the tool removes its validated private staging directory. I choose a target filesystem that supports same-directory atomic rename.
- A missing `cascade-cluster-snapshot.manifest` means the coordinated generation did not publish and is not restorable as a cluster set.
- A cluster-position, directory-ID, or high-watermark mismatch means the controller correctly refused a non-converged generation. I restore availability and retry with a new target rather than editing artifacts.
- A retention verification failure means no generation was deleted. I quarantine the damaged generation and recover it from another verified copy.

## Restore drills and current evidence

I run a restore drill after changing storage formats or maintenance code and on a regular operations schedule. The offline automated end-to-end test produces 100 ordered Kafka records, restores them into a new directory, and consumes the exact values. The coordinated integration test writes 50 records to a three-voter cluster, captures every voter, requires high watermark 50 on each ISR artifact, verifies the collected generation, and proves dropped prepare/capture calls cannot publish a global manifest. Unit and command tests cover leases, malformed wire frames/manifests, position fencing, tampering, extra files, unsafe paths, retention ordering, and corrupt-generation fail-closed behavior.

These tests prove the implemented format, coordination safety, and local recovery path. They do not replace physical power-loss testing, lost-device recovery, a recurring encrypted off-host transfer and full-cluster restore campaign, or multi-terabyte restore timing. I track those remaining gates in [production-readiness.md](production-readiness.md).

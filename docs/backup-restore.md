# Backup and restore runbook

I use Cascade's maintenance commands for offline, file-exact disaster-recovery copies and the broker write barrier for an online point-in-time local snapshot. A rolling set of node snapshots is not automatically one cluster-wide point in time.

## What the backup contains

The tool copies every regular file under `--data-dir`, including topic segments, indexes, high-watermark checkpoints, coordinator journals, cluster metadata, and the clean-shutdown marker. It does not follow symbolic links and refuses an unsupported file type.

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

For a cluster-consistent disaster-recovery set I quiesce writes and stop every broker cleanly before backing up every node. A rolling per-node backup can still be useful for local recovery, but it may contain different committed moments and I do not treat it as an atomic cluster snapshot. A cluster-wide barrier and cross-node manifest remain release follow-ups.

## Create an online broker snapshot

The embedded broker API exposes `createOnlineSnapshot(target)`. It takes the broker's exclusive snapshot barrier, waits for admitted requests to finish, pauses lifecycle and background-flush workers, forces every local partition and coordinator file, publishes the checksummed snapshot, and then resumes traffic. The automated test proves the artifact contains every record acknowledged before the barrier and none acknowledged after it.

This barrier is local to one broker. For replication factor three I must collect an artifact from every replica host at the same operational checkpoint and preserve their node identities. Until Cascade has a cluster snapshot coordinator and a manifest spanning those artifacts, I do not call independently triggered node snapshots an atomic cluster backup.

## Protect and retain backups

The built-in format provides integrity, not confidentiality. I encrypt backup storage, restrict it to the recovery identity, keep at least one copy off the broker host and failure domain, and use immutable/object-lock retention where available. I never edit a published backup in place.

Scheduling and retention are external in this milestone. My job scheduler creates a new target name, checks the command exit status, runs `verify-backup`, transfers the complete directory, verifies the transferred directory again, and deletes an old generation only after the retention policy says another verified generation has replaced it.

## Restore

I restore only into a path that does not exist:

```powershell
.\sbt.bat "run verify-backup --backup-dir E:\cascade-backups\node-1-2026-08-24T1815Z"
.\sbt.bat "run restore --backup-dir E:\cascade-backups\node-1-2026-08-24T1815Z --data-dir D:\cascade\restored-node-1"
```

Restore verifies the manifest and exact file set before copying. It verifies length and SHA-256 again for every staged file, confirms that the source backup did not change, forces the restored files, and atomically publishes the destination. It does not copy the backup manifest into the broker data directory.

Before startup I restore the matching external configuration and secrets, verify ownership/permissions, confirm the node ID and listener addresses, and make sure the original broker with that node identity cannot start at the same time. I start Cascade against the restored directory, expect `recovery=Clean`, wait for readiness, and verify topic metadata, end offsets, representative records, consumer offsets, and transaction visibility through a Kafka client.

For a full-cluster restore I keep the node identities and their corresponding data/config backups paired. I isolate the restored cluster from the original network, restore every intended voter and data replica, start the initial voters, wait for controller/readiness convergence, start remaining nodes, and run exact application-level verification before admitting producers. I do not combine arbitrary node directories from different backup generations without first reasoning about their committed metadata and partition coverage.

## Failure interpretation

- `backup requires a cleanly stopped broker` means I must recover and cleanly stop the source; I do not remove or manufacture marker files.
- `backup source changed` or `broker started while the backup was running` means the copy is discarded and I repeat the procedure after stopping the broker.
- `contents do not match the manifest`, `length mismatch`, or `checksum mismatch` means the backup is corrupt or was modified. I quarantine it and use another verified generation.
- `target already exists` is a safety boundary. I choose a new empty path rather than asking the tool to overwrite data.
- An atomic-move failure leaves no published target; the tool removes its validated private staging directory. I choose a target filesystem that supports same-directory atomic rename.

## Restore drills and current evidence

I run a restore drill after changing storage formats or maintenance code and on a regular operations schedule. The automated end-to-end test currently produces 100 ordered Kafka records, cleanly stops the broker, creates and verifies a backup, restores it to a new directory, starts with clean recovery, and consumes the exact 100 values. Unit tests also tamper with content, add untracked files, exercise traversal and duplicate paths, and confirm existing-target refusal.

That test proves the implemented format and local recovery path; the online test also proves one broker's write barrier and exact point-in-time restore. Neither replaces physical power-loss testing, lost-device recovery, encrypted off-host transfer qualification, multi-terabyte restore timing, or a coordinated cluster-wide snapshot. I track those remaining gates in [production-readiness.md](production-readiness.md).

# Rolling upgrade and downgrade runbook

I qualify a release against a real historical binary instead of changing a version string on the current code. The 1.0.0 baseline is commit `c61264bf304719403b77c9b60709801be544373e`; I keep that version, revision, and its original 290-test evidence in `compatibility/releases/1.0.0.properties`.

## Run the qualification

I use Java 21 or newer and run:

```powershell
./scripts/qualify-rolling-upgrade.ps1 -Report artifacts/rolling-upgrade.json
./scripts/qualify-rolling-upgrade.ps1 -Baseline format9 -Report artifacts/rolling-format9.json
./scripts/qualify-rolling-upgrade.ps1 -Baseline format10 -Report artifacts/rolling-format10.json
./scripts/qualify-rolling-upgrade.ps1 -Baseline format11 -Report artifacts/rolling-format11.json
```

The script creates an isolated detached worktree at the selected pinned revision, builds that historical runtime and the current runtime, and starts three separate broker JVMs with replication factor three, minimum ISR two, synchronous flushing, and independent data directories. It never edits the historical source. Temporary worktrees and broker data are removed after the run unless I add `-KeepData`.

CI runs the same script with full Git history and archives the JSON report. The report records both revisions, SHA-256 for both application jars, elapsed time, exact record count, every completed phase, rollback status, feature activation, and downgrade rejection.

## What the gate proves

The original 1.0.0-to-1.1.0 campaign below is historical. The runner now targets the current `VERSION`, commits consumer offsets during each traffic phase, and verifies those offsets after recovery. It retains non-idempotent `acks=all` traffic because historical binaries restrict anonymous producer initialization to the controller. Current-binary idempotence has separate tests. CI also selects the pinned format-9 source predecessor with `-Baseline format9`; that predecessor is not the published format-8 Docker Hub image.

The historical campaign:

1. Starts three pinned 1.0.0 brokers and verifies replicated `acks=all` traffic.
2. Upgrades broker 3 to 1.1.0 while the other two brokers remain on 1.0.0.
3. Rolls broker 3 back to 1.0.0 before feature activation and verifies continued traffic.
4. Upgrades brokers 3, 2, and 1 one at a time, checking readiness and exact offsets after every replacement.
5. Commits a metadata mutation after every voter advertises 1.1.0, then verifies that every broker persisted the complete activated feature map.
6. Attempts to start 1.0.0 on the activated metadata. The old process must terminate and report an unsupported metadata format; briefly binding its socket during startup is not considered successful admission.
7. Keeps producing on the two-node majority, restores broker 3 on 1.1.0, and consumes the complete ordered sequence without a missing, duplicate, or corrupt value.

The local 2026-09-01 campaign passed all ten phases in 8.278 seconds and consumed exactly 40/40 records. It also found and fixed an eager-format persistence defect: before feature activation, current brokers now write the minimum metadata format required by the committed image instead of silently rewriting rollback-compatible format 6 as format 8.

After adding coordinator deltas and format 9, I repeated this campaign on 2026-09-02: all ten phases passed in 7.927 seconds with exact 40/40 records. The current application jar SHA-256 was `63ff32351f8e63952d67537693bdd47cde6201af0f3932133ed4d99dc9e6a45e`. The working-tree marker in that report reflects documentation edits on top of tested implementation revision `8975fffa3b6eaf6a6d26aefcfb5958abbf66e038`.

## Operational boundary

For 1.2.0 at `fbe4e98a7b9c55680a365453041ee084c0a6efbf`, I reran both baselines on 2026-09-02. The format-9 source predecessor passed all ten phases in **8.933 seconds**; 1.0.0 passed in **8.653 seconds**. Each verified exactly **40/40** ordered records and committed consumer offset **40**, preserved the baseline feature map before activation, accepted pre-activation rollback, and rejected downgrade after activation. I retain the [reports and artifact hashes](performance/2026-09-02-incremental-coordinator.md). These source-binary runs do not qualify the published format-8 Docker image or cross-version idempotent-producer retries.

I can roll a broker back only while the quorum feature map and metadata floor still fit the baseline binary. For the 1.0.0 baseline this means an empty feature map and format 6. For the pinned format-9 predecessor, the existing feature map stays active but `incremental-coordinator` must remain inactive.

The original 1.1.0 feature set requires format 8. `coordinator-deltas` raises the floor to format 9; `incremental-coordinator` raises it to format 10 and permits incremental journal/peer records; `shard-object-storage` raises it to format 11; and `independent-coordinator` raises it to format 12. Each feature activates only when every voter supports it. A format-8 peer keeps whole-image coordinator commits; a format-9 peer permits shard proposals but keeps full-image persistence/replication. Format 12 moves coordinator writes to forced per-shard quorum journals. Once a new floor is active, I roll forward. I never bypass the format check or edit either metadata or coordinator-shard journals. If I must return to an older binary after activation, I restore a verified pre-activation cluster backup into an isolated environment and follow the disaster-recovery runbook.

The current source release is `1.16.0`; its [release notes](releases/1.16.0.md) record source qualification and local-image status separately from Docker Hub publication. Reclaimable quotas remain on the conservative level-1 path until every voter advertises and commits `distributed-quotas` level 2. Coordinated snapshots remain unavailable until every voter advertises and commits `cluster-snapshot` level 1. `-Baseline format10` pins the 1.2.0 source at `fbe4e98a7b9c55680a365453041ee084c0a6efbf`; its existing features remain active during a mixed cluster, while `shard-object-storage` must remain inactive. Unanimous activation raises the floor to format 11. Backup/restore and offline inspection must include the sibling shard-object directory.

The independent coordinator work is newer than the published 1.3.1 image. A mixed cluster must keep format 12 inactive until every voter runs a binary that understands the new internal phase APIs and `.cascade/coordinator-quorum` journals. After activation, rollback to format 11 is unsafe.

Kafka transaction protocol V2 has its own cluster feature, `transaction.version=2`. ApiVersions continues to advertise the implemented Produce/EndTxn/TxnOffsetCommit schemas during a rolling upgrade, but it does not publish V2 as finalized while any voter lacks level 2. Kafka producers therefore keep the explicit V1 partition/group enrollment path throughout the mixed phase. The full-voter commit that finalizes level 2 closes that rollback window because new active and completed delivery images persist the V2 protocol and returned epoch.

Replicated cleanup uses `replicated-cleanup=1` and the same full-voter activation barrier. A mixed cluster keeps the previous local cleanup path and does not send the new internal prepare/commit APIs. After activation, every partition may persist `replicated-cleanup.checkpoint`; I do not roll a broker back to a binary that cannot validate that file and protocol. Before activation I may roll back normally after checking that the feature is absent from the committed map.

On 2026-09-12 I ran the pinned format-11 source at `c0ff51721bd74ea7f3f2172e9758a34499ae9a2e` against 1.4.0 implementation revision `67dafc6da7c3931d0b1765e7fbb86196c070a1ea`. The campaign rolled one broker forward and back before activation, upgraded every voter, activated format 12, rejected a format-11 downgrade, kept traffic on the majority, restored the broker on 1.4.0, and consumed exactly 40/40 ordered records with committed offset 40. The first attempt exposed a stale owner at activation; conflict-driven voter-baseline reconciliation fixed it, and the clean rerun completed all ten phases in 10.365 seconds. A published-image campaign is still mandatory before I publish 1.4.0.

The 2026-09-02 shard-storage campaign passed all three source baselines against 1.3.0-SNAPSHOT: format 10 in 9.559 seconds, format 9 in 8.877 seconds, and 1.0.0 in 8.953 seconds. Each completed ten phases and exact 40-record/offset checks, including pre-activation rollback and post-activation downgrade rejection. I archive [all reports and artifact hashes](performance/2026-09-02-shard-storage.md).

## Adjacent release image gate

The source-runtime campaigns prove storage-format compatibility, but they do not execute either release image's entry point, minimal JRE, non-root identity, health check, read-only filesystem, native libraries, or OCI labels. I qualify each adjacent release pair separately with the actual immutable image IDs:

```powershell
.\scripts\qualify-adjacent-images.ps1 `
  -BaselineImage miladsade96/cascade:1.15.0 `
  -BaselineVersion 1.15.0 `
  -CandidateImage miladsade96/cascade:1.16.0 `
  -CandidateVersion 1.16.0 `
  -Report artifacts/adjacent-images-1.15.0-to-1.16.0.json
```

The gate creates an isolated Docker network and one persistent volume per voter, pins both tags to inspected SHA-256 image IDs, and rejects a wrong version, mutable identity, root user, missing health check, missing creation time, or non-Git revision. It starts three baseline brokers with replication factor three, minimum ISR two, synchronous flushing, read-only roots, dropped capabilities, and `no-new-privileges`.

One Kafka producer keeps the same idempotent session through the entire campaign. The gate upgrades a non-leader, rolls it back before the full upgrade, upgrades it again, and then replaces the remaining non-leader. It submits a larger in-flight record wave while abruptly removing the baseline partition leader and replacing it with the candidate image. The run fails unless Kafka reports at least one producer retry and every acknowledgement retains its planned contiguous offset. After all nodes run the candidate, it proves the expected compatible downgrade on one non-leader, restores that node, restarts all three candidate containers from their original volumes, and verifies every value and the exact committed consumer offset.

The atomic JSON report binds versions, Git revisions, image IDs, elapsed time, phase list, retry count, leader node, rollback/downgrade outcomes, exact recovery, and final record count. Unique resource names prevent one run from deleting another. Cleanup targets only those validated names; `-KeepResources` preserves them for diagnosis. A failed or missing report is never converted into a pass.

This adjacent pair does not introduce a new storage-format floor, so a 1.15.0 broker is expected to rejoin after the cluster has run 1.16.0. For a future release that activates an incompatible feature, I will change the pair's declared downgrade expectation and require the baseline to fail closed instead. I never infer downgrade safety from semantic version numbers alone.

On 2026-10-21 this gate passed all ten phases in 96.321 seconds with 1,200 exact records, committed offset 1,200, 100 measured producer retries, rollback before the full upgrade, the declared compatible downgrade after it, and exact recovery after restarting all three candidate brokers. I keep the complete environment, image identities, discovered harness defects, and limitations in the [adjacent image rolling report](performance/2026-10-21-adjacent-image-rolling.md).

## Production procedure

Before a rolling release I verify a current backup, record image digests, keep the old image available, and hold unrelated metadata administration. I replace one non-controller broker at a time, wait for readiness and ISR recovery, and verify `acks=all` traffic before continuing. I replace the controller last.

I use the pre-activation window for rollback validation. Once all brokers are on the new binary, I allow a controlled metadata mutation, confirm activation and readiness on every broker, and declare the rollback window closed. The automated source gate now also covers the pinned format-11 baseline against 1.4.0. Every published adjacent image pair still needs its own archived campaign.

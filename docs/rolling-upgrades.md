# Rolling upgrade and downgrade runbook

I qualify a release against a real historical binary instead of changing a version string on the current code. The 1.0.0 baseline is commit `c61264bf304719403b77c9b60709801be544373e`; I keep that version, revision, and its original 290-test evidence in `compatibility/releases/1.0.0.properties`.

## Run the qualification

I use Java 21 or newer and run:

```powershell
./scripts/qualify-rolling-upgrade.ps1 -Report artifacts/rolling-upgrade.json
./scripts/qualify-rolling-upgrade.ps1 -Baseline format9 -Report artifacts/rolling-format9.json
```

The script creates an isolated detached worktree at the pinned revision, builds the real 1.0.0 runtime, builds the current runtime, and starts three separate broker JVMs with replication factor three, minimum ISR two, synchronous flushing, and independent data directories. It never edits the historical source. Temporary worktrees and broker data are removed after the run unless I add `-KeepData`.

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

I can roll a broker back only while the quorum feature map and metadata floor still fit the baseline binary. For the 1.0.0 baseline this means an empty feature map and format 6. For the pinned format-9 predecessor, the existing feature map stays active but `incremental-coordinator` must remain inactive.

The original 1.1.0 feature set requires format 8. `coordinator-deltas` raises the floor to format 9; `incremental-coordinator` raises it to format 10 and permits incremental journal/peer records. Each feature activates only when every voter supports it. A format-8 peer keeps whole-image coordinator commits; a format-9 peer permits shard proposals but keeps full-image persistence/replication. Once the new floor is active, I roll forward. I never bypass the format check or edit the metadata journal. If I must return to an older binary after activation, I restore a verified pre-activation cluster backup into an isolated environment and follow the disaster-recovery runbook.

The new source is now versioned 1.2.0 with a local Docker image, but I have not published it to Docker Hub. Earlier qualification artifacts used the 1.1.0 version string, so I identify them by Git revision and jar/image digest. The 1.1.0-to-1.2.0 boundary still needs a pinned format-8 predecessor campaign before I call that adjacent release pair qualified; building a new image does not provide that evidence.

## Production procedure

Before a rolling release I verify a current backup, record image digests, keep the old image available, and hold unrelated metadata administration. I replace one non-controller broker at a time, wait for readiness and ISR recovery, and verify `acks=all` traffic before continuing. I replace the controller last.

I use the pre-activation window for rollback validation. Once all brokers are on the new binary, I allow a controlled metadata mutation, confirm activation and readiness on every broker, and declare the rollback window closed. The automated gate covers the pinned 1.0.0-to-1.1.0 boundary; every later release pair needs its own pinned baseline and archived report.

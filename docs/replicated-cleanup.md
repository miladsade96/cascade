# Replicated partition cleanup

I coordinate retention and compaction per partition instead of letting every replica make a wall-clock decision independently.

## Protocol

The current partition leader calculates one deterministic plan under the same partition lock used by Produce and replica recovery. The plan contains the exclusive delete boundary, the exclusive compaction boundary, and one tombstone cutoff timestamp. It is fenced by the leader epoch and a generation that increases within that epoch.

Before any replica changes a segment, every current ISR member must validate and durably prepare the exact plan. A missing, timed-out, stale-epoch, non-boundary, or beyond-high-watermark response blocks the leader from committing and applying it. After the complete prepare barrier, the leader forces its commit checkpoint, sends the commit to the ISR, and applies the same plan locally. Commit delivery and application are idempotent, so a missed reply is retried by later lifecycle passes.

Each partition stores `replicated-cleanup.checkpoint`. It is atomically replaced and CRC32C protected, and records prepared, committed, and applied decisions separately. Startup never applies a prepared-only generation. It resumes a committed but unapplied generation exactly once, while an interrupted physical application can safely repeat the same boundaries.

## Failover and recovery

A promoted leader uses its newer leader epoch to supersede an uncommitted generation. Stale leaders and stale generations are rejected. Replica recovery copies the committed record prefix first, then transfers the leader's committed cleanup decision before the controller can return that replica to the ISR. When the decision belongs to an older leader epoch, the current leader re-certifies the same plan in its own epoch.

The recovery rule matters when a returning replica still has an old prefix that the live ISR already reclaimed. Suffix repair alone would leave that prefix behind. Cleanup-state transfer removes it through the normal validated plan instead of treating different local segment names as proof of divergence.

## Rolling activation

The internal prepare/commit APIs are guarded by the `replicated-cleanup=1` cluster feature. It activates only after every voter advertises support and the full-voter metadata commit succeeds. Before activation, a mixed cluster stays on the previous local lifecycle behavior. After activation, rollback to a binary that does not understand the cleanup checkpoint or peer APIs is unsafe.

## Operational response

I do not delete `replicated-cleanup.checkpoint` to force progress. A checksum or structural failure means I isolate that replica, preserve its partition directory and logs, check the device/filesystem, and recover it from a verified in-sync replica. Repeated prepare failures mean I inspect peer reachability, leader epoch, ISR membership, disk health, and the lifecycle policy before changing any files.

This protocol coordinates software cleanup decisions; it does not qualify the storage device against torn atomic renames, lost flushes, firmware faults, or physical power loss. Those remain separate release gates.

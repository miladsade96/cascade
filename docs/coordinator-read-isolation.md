# Acknowledged coordinator read isolation

I use immutable acknowledged views to keep coordinator reads available while a write is waiting for quorum publication. This removes the shared group/delivery publication monitor from `OffsetFetch`, `ListGroups`, `DescribeGroups`, and `read_committed` visibility decisions. Format 12 now provides the [independent shard quorum](coordinator-architecture.md), but it does not remove the broker-local write-side mutation lock.

## Offset view

I keep staged offsets in the mutable coordinator image used to prepare a checkpoint, but readers use a separate volatile `OffsetReadView`. The view has an exact key index and a deterministic per-group index. A successful checkpoint publishes one replacement view; a failed checkpoint leaves the previous view in place. Installing an authoritative coordinator image replaces the mutable state and acknowledged view together.

Updates rebuild only the groups they touch. Unchanged group vectors are structurally shared, and a no-op returns the same view. Offset expiry follows the same rule: removing an offset is invisible until its checkpoint succeeds. A reader captures one view after one readiness decision, so all partitions in one `OffsetFetch` response come from the same acknowledged generation.

## Transaction view

I derive `DeliveryReadView` from the last acknowledged delivery image. It indexes the earliest active offset by topic-partition and completed outcomes by producer ID and epoch. Committed transactional data is visible only when the newest covering outcome is committed and its transactional offsets were applied atomically.

The Fetch handler captures one delivery view for the whole response. Last-stable-offset calculation and every transactional batch decision use that same object, even if a transaction completes while the response is being built. Transaction expiry remains scheduled on the write side; a late expiry can conservatively withhold data, but it cannot expose an uncommitted batch.

## Group administration view

I derive one sorted, detached `GroupAdminReadView` from the acknowledged group image. It covers classic groups, consumer-protocol groups, and groups that only retain offsets. List state filters and every requested description use this published view; a blocked or rejected checkpoint cannot expose a tentative member, assignment, state, or deletion.

A successful empty-group deletion removes the group and its offsets in the same checkpoint. The old view remains readable until acknowledgement, then both disappear together. The full wire behavior, ACL contract, and deletion safeguards are in [consumer group administration](group-administration.md).

## Failure contract

- A write waiting for quorum is never visible through the acknowledged view.
- A successful publication changes visibility from the old complete view to the new complete view in one reference replacement.
- A rejected or failed publication leaves readers on the old complete view.
- A remote authoritative install cannot expose a partially decoded or partially applied image.
- Read availability does not imply ownership availability. The request still makes one coordinator readiness decision and returns the Kafka coordinator error when that decision fails.

## Verification

The deterministic unit tests pause a checkpoint after staging and prove that direct offset, full-group offset, group administration, last-stable-offset, and transaction visibility reads finish before the checkpoint is released. They cover both accepted and rejected publications, old-view immutability, producer-epoch fencing, newest-outcome precedence, pending transactional offsets, index removal, duplicate replacement, and concurrent counters.

The three-broker wire test selects a group owned by a non-controller broker, pauses that owner's shard-finalize request to one follower, and issues a real Kafka `OffsetFetch`. The read returns the last acknowledged offset within a fixed deadline. After the pause is released and finalization commits, the next read returns the new offset.

The scale runner and broker expose node-only counters for offset and delivery reads:

- `cascade_coordinator_offset_read_snapshots_total`
- `cascade_coordinator_offset_read_keys_total`
- `cascade_coordinator_stable_offset_snapshots_total`
- `cascade_coordinator_transaction_visibility_snapshots_total`

Group administration adds six more node-only counters for list snapshots/entries, describe snapshots/hits, and delete attempts/failures. I keep their exact names and interpretation in the [group administration runbook](group-administration.md).

I do not add group, topic, partition, transactional ID, or producer labels. The release gate requires the real paused-RPC test, and the 1,000-group campaign requires acknowledged offset views before and after controller failover.

## Remaining boundary

This milestone narrows read/write contention for committed offsets, group administration, and transaction visibility. Format 12 now uses forced per-shard quorum journals instead of metadata-quorum publication for steady-state coordinator writes. Group joins, heartbeats, rebalances, deletion mutation, transaction mutation, checkpoint preparation, and installation still share broker-local write-side coordination. Distributed resolution of decided transactions, journal checkpoint compaction, membership and transaction churn qualification, arbitrary network impairment, and dedicated-host RF=3 capacity evidence remain production gates.

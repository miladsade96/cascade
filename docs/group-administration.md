# Consumer group administration

I expose the Kafka 4.3.1 consumer administration surface: `ListGroups` v0-v5, `DescribeGroups` v0-v4, `DeleteGroups` v0-v2, `ConsumerGroupDescribe` v0-v1, `OffsetFetch` v4-v10, and `OffsetDelete` v0. These APIs cover classic groups, modern consumer groups, and groups that only retain committed offsets. The exact version ranges remain part of the compatibility contract.

## Acknowledged view

List and describe read one immutable group view published at the coordinator checkpoint boundary. A mutation is not visible while its local or quorum checkpoint is unresolved. When the checkpoint succeeds, the group image and its offsets become visible together; when it fails, neither becomes visible. Administration reads do not wait on an in-flight coordinator publication.

The view is detached from mutable member state and sorted by group ID. Classic descriptions include the selected protocol, member metadata, assignment, client ID, and static instance ID. Offset-only groups appear as `Empty` with an empty protocol type. Modern descriptions include group and assignment epochs, state, assignor, member and instance IDs, rack, client ID and host, explicit or regex subscriptions, and both the current and target assignment.

`ListGroups` v4 applies Kafka's state filter against the same snapshot. Version 5 also filters and reports `Classic` or `Consumer` group types. In a cluster, each broker only reports the group IDs it currently owns. Kafka Admin collects the broker-local results to build a cluster-wide listing.

`OffsetFetch` v6-v10 uses flexible encoding, version 8 reads several groups in one request, version 9 validates modern member identity and epoch, and version 10 uses topic IDs. Membership validation and offsets come from immutable acknowledged images, so a paused write cannot block or expose a tentative offset read.

## Authorization

With ACLs enabled, listing returns only groups for which the principal has `Describe`. Both description APIs return `GROUP_AUTHORIZATION_FAILED` for a denied group. Offset reads and commits require `Read`; group and selected-offset deletion require `Delete`. Coordinator discovery returns Kafka authorization errors on the wire instead of dropping the connection.

For example:

```text
allow alice Describe Group reporting-*
allow alice Delete Group reporting-old
```

The first prefixed rule makes matching groups visible and describable. The literal second rule permits deletion of one group. I keep group IDs out of Prometheus labels so the metric cardinality remains bounded.

## Deletion contract

Delete is deliberately conservative:

- a classic or consumer-protocol group with live members returns `NON_EMPTY_GROUP`;
- an unknown group returns `GROUP_ID_NOT_FOUND`;
- a request sent to the wrong owner returns `NOT_COORDINATOR` so the client can rediscover it;
- a successful deletion removes the empty group and all of its committed offsets in one checkpoint;
- failed publication leaves the last acknowledged group and offsets intact.

The deletion record survives journal compaction, broker restart, full cluster restart, and coordinator failover. A client timeout can still make a completed administrative mutation look ambiguous, so callers should re-describe the group before retrying a destructive request.

`OffsetDelete` removes only the requested committed offsets. It returns `GROUP_SUBSCRIBED_TO_TOPIC` for a topic still used by an active member, preserves unrelated offsets, and publishes the removal through the same atomic checkpoint. Kafka Admin exposes this as `deleteConsumerGroupOffsets`.

## Operations

I export these node-scoped counters:

| Metric | Meaning |
| --- | --- |
| `cascade_coordinator_group_list_snapshots_total` | acknowledged list snapshots read |
| `cascade_coordinator_group_list_entries_total` | group entries returned |
| `cascade_coordinator_group_describe_snapshots_total` | acknowledged descriptions read |
| `cascade_coordinator_group_describe_hits_total` | requested groups found |
| `cascade_coordinator_group_delete_attempts_total` | deletion attempts admitted to a coordinator |
| `cascade_coordinator_group_delete_failures_total` | deletion attempts rejected or not acknowledged |

The bundled Grafana dashboard graphs their five-minute rates. A sustained delete-failure ratio is a signal to inspect ACL denials, active membership, coordinator movement, and checkpoint health. It is not useful to retry `NON_EMPTY_GROUP` until the members have left or expired.

## Qualification and remaining work

The test boundary includes byte-level flexible and legacy frames, Kafka 4.3.1 Admin lifecycle tests, modern detailed descriptions, state and type filters, missing and active groups, selected and full deletion, topic IDs, member-epoch fencing, backward state decoding, broker restart, and three-broker coordinator failover. The 2026-09-13 full run passed 552/552 tests after a real-client check found and fixed one assignment-structure framing defect and the full suite found and fixed one blocking membership-read path.

High-cardinality multi-day rebalance churn and dedicated-host administration capacity are still open. Format 12 supplies the independent shard quorum, including distributed decision resolution and shard-journal compaction. The remaining operational qualification is tracked in the [production-readiness checklist](production-readiness.md), and the old-or-new read boundary is described in [coordinator read isolation](coordinator-read-isolation.md).

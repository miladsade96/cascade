# Consumer group administration

I expose the Kafka group administration APIs needed by Kafka 4.3.1 `Admin`: `ListGroups` v0-v4, `DescribeGroups` v0-v4, and `DeleteGroups` v0-v1. They cover classic groups, `ConsumerGroupHeartbeat` v0 groups, and groups that only retain committed offsets. This is an explicit compatibility range, not a claim that every Kafka group API is implemented.

## Acknowledged view

List and describe read one immutable group view published at the coordinator checkpoint boundary. A mutation is not visible while its local or quorum checkpoint is unresolved. When the checkpoint succeeds, the group image and its offsets become visible together; when it fails, neither becomes visible. Administration reads do not wait on an in-flight coordinator publication.

The view is detached from mutable member state and sorted by group ID. Classic descriptions include the selected protocol, member metadata, assignment, client ID, and static instance ID. Cascade does not persist a client network address, so the legacy `client_host` field is empty. Offset-only groups appear as `Empty` with an empty protocol type. Consumer-protocol groups are identified with protocol type `consumer`; detailed modern member inspection still belongs to the not-yet-implemented `ConsumerGroupDescribe` API.

`ListGroups` v4 applies Kafka's state filter against the same snapshot. In a cluster, each broker only reports the group IDs it currently owns. Kafka Admin collects the broker-local results to build a cluster-wide listing.

## Authorization

With ACLs enabled, listing returns only groups for which the principal has `Describe`. Describe returns `GROUP_AUTHORIZATION_FAILED` for a denied group. Delete requires `Delete` on every requested group and returns a result for each ID. Coordinator discovery returns Kafka authorization errors on the wire instead of dropping the connection.

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

The test boundary includes byte-level protocol cases, Kafka 4.3.1 Admin lifecycle tests, state filters, missing and active groups, per-group ACLs, deletion with offset cleanup, broker restart, and three-broker coordinator failover. The complete Scala suite and external client matrix remain release gates.

Later `ConsumerGroupHeartbeat` versions, `ConsumerGroupDescribe`, `ListConsumerGroupOffsets`, `DeleteConsumerGroupOffsets`, high-cardinality rebalance churn, and dedicated-host administration capacity are still open. Format 12 now supplies the independent shard quorum; distributed decision resolution, shard-journal compaction, and finer-grained mutation locks remain on the [production-readiness checklist](production-readiness.md). The old-or-new read boundary is described in [coordinator read isolation](coordinator-read-isolation.md).

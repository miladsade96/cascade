# ADR 0003: ISR-based synchronous replication

- Status: Accepted
- Date: 2026-09-19

## Context

Replicated partitions need a clear rule for leadership, acknowledgement, visibility, and recovery. I also need behavior that ordinary Kafka producers understand through `acks=all`, leader epochs, and familiar retriable errors.

## Decision

Partition metadata stores replicas, the in-sync replica set, leader, and leader epoch. The leader appends locally and sends bounded record-batch replication requests to followers. For `acks=all`, Cascade requires `min.insync.replicas` and waits for every current ISR member to append before advancing the committed high watermark and acknowledging.

When a leader fails, the controller removes it from ISR and promotes a surviving ISR member with a higher epoch. A returning replica probes the leader, truncates divergent data, catches up, and is admitted to ISR only after validation.

## Alternatives considered

- Asynchronous follower replication would improve leader latency but weaken the meaning of acknowledged data during failover.
- Majority-per-record consensus would provide a different safety model but would not match Kafka's ISR and client expectations.
- Unclean leader election could improve availability with no ISR member, but it can lose acknowledged records and is not enabled.

## Trade-offs

Waiting for the whole ISR makes a slow replica visible in `acks=all` latency. Removing failed replicas restores progress but reduces redundancy until recovery completes.

## Consequences

Capacity claims must state replication factor, minimum ISR, acknowledgement mode, force policy, and storage hardware. Failure tests verify exact values and offsets, not merely that the cluster accepts new traffic.


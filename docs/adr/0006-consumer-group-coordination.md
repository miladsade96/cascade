# ADR 0006: Sharded acknowledged consumer coordination

- Status: Accepted
- Date: 2026-09-19

## Context

Cascade needs to serve existing classic consumers and Kafka's newer broker-assigned consumer protocol. Group state changes frequently, must survive failover, and should not force unrelated groups through one global metadata mutation path.

## Decision

I support classic Join/Sync/Heartbeat state and modern member-epoch, subscription, and assignment state in one coordinator image. Rendezvous hashing routes a group to an eligible broker. Group state is split into 64 virtual shards; format-12 changes prepare, vote, certify, and finalize through independent forced shard journals on a voter majority.

Reads use immutable acknowledged snapshots with per-group and per-offset indexes. Mutations remain serialized within the group coordinator, while reads do not take its mutation monitor. Offset commits use bounded FIFO batching with ownership and member revalidation before commit.

## Alternatives considered

- One controller-owned group map was easier but concentrated ownership and publication work.
- Hash modulo broker count moves most keys whenever membership changes; rendezvous hashing limits movement.
- Serving tentative mutable state would reduce copy work but could expose decisions that quorum later rejects.

## Trade-offs

Virtual sharding improves conflict isolation but does not make every mutable operation lock-free. High-cardinality churn still needs dedicated-host and multi-day qualification.

## Consequences

Classic and modern protocol paths share durable offsets and authorization while retaining their own fencing rules. Failover tests verify acknowledged views, assignments, member epochs, static identities, and exact offsets.


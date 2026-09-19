# ADR 0008: Virtual-thread connection model

- Status: Accepted
- Date: 2026-09-19

## Context

Kafka connections are long-lived and request processing can block on storage, replication, coordinator quorum, TLS, or quotas. A platform thread per connection would consume too many native resources, while a selector/worker architecture increases state-machine complexity.

## Decision

Cascade uses Java 21 virtual threads for connection and peer-request isolation. Requests remain ordered within one client connection. Persistent peer connections avoid reconnecting for every replication or controller request.

Virtual threads do not replace admission control: the broker bounds global/per-IP connections, in-flight requests, frame sizes, coordinator queues, and per-principal traffic. TLS/SASL identity and authorization remain connection-scoped.

## Alternatives considered

- One platform thread per connection is simple but scales poorly in native memory and scheduling overhead.
- A non-blocking selector with explicit request state machines can be extremely efficient but would increase complexity across TLS, SASL, and ordered responses.
- An unbounded virtual-thread design would be concise but would move overload into heap, sockets, storage, or controller queues.

## Trade-offs

Virtual threads reduce orchestration code but do not make blocking work free. Tiny batches with exact distributed quotas can still make controller RPC rate the bottleneck. CPU-bound work may eventually benefit from profiled worker pools.

## Consequences

Load tests record CPU, heap, queue rejection, latency, and connection counts. I will adopt selector pools or zero-copy changes only when profiles show that the simpler model is the limiting factor.


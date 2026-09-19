# ADR 0001: Kafka wire protocol compatibility

- Status: Accepted
- Date: 2026-09-19

## Context

I wanted Cascade to work from ordinary programming languages without maintaining a proprietary SDK for each one. Kafka already has a documented, versioned binary protocol and mature clients. Claiming blanket Kafka compatibility would be misleading because each API version has its own schema and semantics.

## Decision

I implement Kafka's length-prefixed TCP framing and an explicit subset of API keys and versions. `ApiVersions` advertises exactly that subset, and the broker rejects unimplemented versions before dispatch. The Scala/JDK runtime contains no Kafka broker dependency; Apache Kafka's Java client and the external language clients remain independent test tools.

Request routing goes through typed domain handlers. Binary codecs perform bounded reads, flexible tagged-field handling where required, and complete-body validation.

## Alternatives considered

- A Cascade-specific HTTP/gRPC protocol would be easier to evolve but would require custom clients and would not prove Kafka interoperability.
- Reusing Kafka broker internals would provide more surface area but would defeat the from-scratch implementation goal.
- Advertising broad version ranges and implementing only common paths would make compatibility appear larger while failing unpredictably.

## Trade-offs

Protocol compatibility is expensive: wire schemas, error codes, throttle fields, coordinator behavior, and version transitions all need independent tests. Supporting a deliberate subset reduces feature breadth but makes each advertised contract defensible.

## Consequences

Every new API/version needs codec, malformed-input, broker integration, and real-client coverage before it is advertised. The compatibility matrix is a release artifact, and unsupported Kafka features remain visible limitations.


# ADR 0005: Durable idempotent producer state

- Status: Accepted
- Date: 2026-09-19

## Context

Kafka producers retry when a response is lost. Without producer identity, epoch fencing, and sequence validation, a successful append can become a duplicate or an old producer can continue after ownership changes.

## Decision

Cascade allocates durable producer IDs and monotonically advances epochs. Each partition validates producer epoch and sequence before append, keeps a bounded five-batch duplicate window, and returns the original offset for a recognized retry. Producer state recovers from persisted record-batch headers after restart.

Producer allocation and fencing belong to the delivery coordinator. An expected ID/epoch initialization retry returns the already-applied result instead of advancing the epoch twice.

## Alternatives considered

- Client-generated UUID keys would require application cooperation and would not implement Kafka idempotence.
- Unbounded sequence history would simplify old retry recognition but leak memory and durable state.
- Memory-only producer state would be fast but would accept duplicates or stale epochs after restart.

## Trade-offs

The bounded window matches the intended retry horizon but cannot recognize arbitrarily old batches. Recovery work scans durable headers and therefore depends on valid segment recovery.

## Consequences

Tests cover duplicate response offsets, gaps, wraparound, epoch fencing, initialization retries, restart recovery, leader failover, and concurrent transaction admission.


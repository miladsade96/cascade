# ADR 0005: Durable idempotent producer state

- Status: Accepted
- Date: 2026-09-19

## Context

Kafka producers retry when a response is lost. Without producer identity, epoch fencing, and sequence validation, a successful append can become a duplicate or an old producer can continue after ownership changes.

## Decision

Cascade allocates durable producer IDs and monotonically advances epochs. Each partition validates producer epoch and sequence before append, keeps a bounded five-batch duplicate window, and returns the original offset for a recognized retry. Producer state recovers by merging persisted record-batch headers with an atomic, versioned, CRC32C-protected per-partition checkpoint. The checkpoint preserves producer ID, epoch, base and last sequence, offset span, record count, timestamp, compression, and transaction/control flags even when retention or compaction removes the source batch.

Producer allocation and fencing belong to the delivery coordinator. An expected ID/epoch initialization retry returns the already-applied result instead of advancing the epoch twice.

## Alternatives considered

- Client-generated UUID keys would require application cooperation and would not implement Kafka idempotence.
- Unbounded sequence history would simplify old retry recognition but leak memory and durable state.
- Memory-only producer state would be fast but would accept duplicates or stale epochs after restart.

## Trade-offs

The bounded window matches Kafka's intended retry horizon but cannot recognize arbitrarily old batches. The extra forced checkpoint occurs only at snapshot, close, or immediately before destructive log maintenance; it does not add a second force to every append. A corrupt checkpoint fails startup instead of silently weakening idempotence.

## Consequences

Tests cover duplicate response offsets, gaps, wraparound, epoch fencing, initialization retries, restart recovery, compaction and retention restart, authoritative replica truncation, leader failover, and concurrent transaction admission.


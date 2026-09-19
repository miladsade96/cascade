# ADR 0002: Segmented append-only log storage

- Status: Accepted
- Date: 2026-09-19

## Context

The broker needs stable offsets, sequential writes, bounded recovery time, retention, compaction, and efficient Fetch responses. Deserializing and reserializing every Kafka record batch would waste CPU and could change client compression semantics.

## Decision

Each topic partition owns an append-only `PartitionLog` made of size-bounded segments. Kafka magic-v2 batches stay serialized and compressed on the normal path; Cascade changes only the base offset outside the batch CRC region. Positional file I/O avoids shared channel-position races. Offset, timestamp, and transaction indexes rebuild from persisted batches when needed.

I support `periodic` batched forcing and `sync` per-append forcing as explicit durability policies. Segment deletion and compaction use atomic rename protocols that startup recovery completes or rolls forward safely.

## Alternatives considered

- One file per partition is simpler but makes retention, compaction, and recovery increasingly expensive.
- A database engine would provide transactions and indexing but hide the storage behavior I need to control and qualify.
- Always decompressing batches would simplify record-level policies but make the hot path compression-aware and increase allocation.

## Trade-offs

Opaque compressed batches preserve throughput and compatibility, but advanced compaction currently rewrites only uncompressed and gzip data. Periodic forcing improves throughput while creating a documented local power-loss window.

## Consequences

Acknowledgement, replication, and disk forcing remain separate contracts. Recovery tests must cover torn batches, later dependent segments, interrupted rename operations, indexes, and high-watermark checkpoints.


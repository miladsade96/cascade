# ADR 0007: Certified transaction coordination

- Status: Accepted
- Date: 2026-09-19

## Context

Transactions span producer identity, multiple partitions, outcome markers, and consumer offsets. A controller failure between decision and publication must not turn a commit into an abort or expose staged offsets early.

## Decision

The delivery coordinator durably tracks producer epochs, active ranges, enlisted partitions/groups, timeouts, outcomes, and offset-application checkpoints. Transaction changes use sorted shard locking and majority-forced prepare, vote, decision-certificate, and terminal records. A successor queries voter decisions and completes a certified in-doubt transaction.

Partitions reserve transaction ranges before append. Fetch calculates the last stable offset and excludes aborted or open ranges for `read_committed`. Transactional offsets become visible only after the commit outcome and applied checkpoint.

## Alternatives considered

- Best-effort markers without a durable coordinator can disagree after failover.
- A single metadata-log append for every transaction was simpler but coupled high-rate delivery state to cluster metadata publication.
- Two independent commits for data outcome and offsets could expose one without the other.

## Trade-offs

Cross-shard certification costs more messages and forced records than a local transaction. Cascade intentionally caps APIs that require Kafka transaction protocol V2 until those semantics exist.

## Consequences

Qualification includes commit/abort visibility, open-transaction last-stable offsets, fencing, timeout aborts, transactional offsets, restart, coordinator loss, certified recovery, and Admin inspection.


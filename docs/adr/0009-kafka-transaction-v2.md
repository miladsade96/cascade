# ADR 0009: Cluster-gated Kafka transaction protocol V2

- Status: Accepted
- Date: 2026-10-15
- Supersedes: the V2 version cap described in [ADR 0007](0007-transactions.md)

## Context

Kafka transaction protocol V2 is not just a newer schema. Produce v12 replaces explicit partition enrollment, TxnOffsetCommit v5 replaces explicit group enrollment, and EndTxn v5 advances and returns the producer identity after every transaction. Enabling only part of that behavior can lose enrollment, reuse a sequence under the wrong epoch, or return a different identity when a response is retried.

A rolling cluster also cannot let one upgraded broker choose V2 while another broker still persists only V1 state. The client decides from finalized feature metadata, so that metadata is part of the safety boundary.

## Decision

Every current broker advertises support for `transaction.version` level 2. The cluster finalizes level 2 only through a full-voter commit. Until then, clients remain on transaction V1 even though the broker's implemented API ranges include the newer wire schemas.

The delivery image stores the protocol version on active and completed transactions. Produce v12 and TxnOffsetCommit v5 perform their enrollment inside the same acknowledged coordinator path as the resulting mutation. EndTxn v5 commits the outcome, removes the active transaction, updates the producer registration, and stores the returned producer ID/epoch atomically. Completion retries look up that stored transition. Epoch exhaustion rotates the producer ID; timeout and reinitialization preserve the same fencing record.

Delivery images have an explicit format header and still decode headerless V1 images. A V1 request cannot mutate a V2 transaction and a V2 request cannot silently adopt a V1 transaction.

## Alternatives considered

- Advertising v5/v12 without finalizing a feature leaves the Kafka client on V1 and does not qualify V2 behavior.
- Inferring the protocol from the latest request loses the choice across restart and failover.
- Advancing the producer epoch after the outcome commit creates a crash window with two valid interpretations.
- Enabling V2 broker by broker allows a transaction to cross incompatible coordinators during a rolling upgrade.

## Trade-offs

Each successful V2 transaction writes a new producer epoch, increasing coordinator write volume. The completion history retains enough identity information for idempotent EndTxn retries. Full-voter activation delays the feature when one voter is unavailable, but it prevents a mixed semantic state.

## Consequences

Qualification covers feature metadata, mixed-version activation, flexible wire schemas, implicit enrollment, commit and abort visibility, repeated epoch resets, overflow rotation, timeout fencing, restart, coordinator failover, transactional offsets, append/publication races, and old-image decoding. `InitProducerId` v6 remains outside this decision because Kafka defines it as a separate unstable two-phase-commit extension.

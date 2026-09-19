# ADR 0004: Durable majority controller election

- Status: Accepted
- Date: 2026-09-19

## Context

Topic, partition, broker, and feature metadata needs one fenced writer while surviving controller loss. An in-memory election can reuse old terms after restart, and a lease without majority contact can leave an isolated controller serving stale decisions.

## Decision

Each voter persists its current term and one vote per term in a forced CRC32C journal. Candidates include their metadata position; voters reject stale candidates. A majority elects the controller, and periodic majority contact maintains its lease. Loss of the lease fences the controller.

Initial deadlines prefer the configured bootstrap controller, while later elections use deterministic node/term jitter. Membership changes use joint consensus and require both old and new majorities until stabilization. Timing and membership projection are separate pure components used by `ClusterManager`.

## Alternatives considered

- A fixed controller is simpler but is a metadata availability single point of failure.
- An external coordination service reduces broker code but adds another distributed system and deployment dependency.
- Wall-clock timestamps are easy fencing tokens but are unsafe under skew and restart.

## Trade-offs

Majority safety deliberately rejects metadata changes during quorum loss. Joint consensus is more complex than replacing the voter list in one step, but prevents two independent majorities.

## Consequences

Controller terms, votes, metadata freshness, lease expiry, network partitions, and every joint-membership phase need deterministic tests. Physical power-loss validation remains a separate hardware gate.


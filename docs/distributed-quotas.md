# Distributed quotas

I configure the request, response, Produce, and Fetch byte rates per authenticated principal. In a level-2 cluster these are cluster-wide limits, not per-broker limits and not permanently divided shares.

## Reservation model

The active metadata controller owns one token bucket for each quota class and principal. Every broker sends a small authenticated peer reservation before it admits client bytes. The controller serializes those reservations against the same bucket, which lets traffic on one broker use capacity that idle brokers never reserved while keeping the aggregate rate and burst bounded. Cascade uses persistent peer connections and virtual-thread request handling; a zero rate bypasses quota coordination entirely.

Ingress reservations may be rejected when the required delay exceeds `--max-throttle-ms`. A rejected reservation restores its tokens. Egress is different because Cascade may already have executed and acknowledged work: it waits the full computed delay instead of leaking response bytes above the configured limit. I size response and Fetch limits so this fail-safe wait does not hold normal client connections for long periods.

## Activation and rolling upgrades

Dynamic reclamation is `distributed-quotas` feature level 2. A mixed cluster at level 1 keeps the old conservative `limit / voter-count` buckets. Cascade requires every voter to commit the level-2 metadata transition before the controller activates the shared ledger; quorum-only activation would allow a lagging broker to spend an old local share at the same time as the new global bucket.

I keep all four rate, burst, and maximum-throttle settings identical on every broker. Each reservation carries the sender's effective setting, and the controller rejects a mismatch without admitting bytes. I compare rendered configuration before a rollout and watch `cascade_distributed_quota_configuration_mismatches_total` during it.

## Failover safety

The ledger is fenced by the durable controller term. An old or isolated controller cannot reserve after its quorum lease expires. A replacement controller discards local predecessor state and starts its term with zero tokens; capacity accrues from that point at the configured rate. This deliberately sacrifices a short amount of availability after failover instead of risking a second burst.

Cascade does not retry an ambiguous forwarded reservation. If a response is lost after the controller accounted for it, the client request fails closed and the reserved capacity returns naturally through token refill. Retrying automatically could double-reserve the request. Internal peer RPCs are authenticated and authorized but excluded from client quota accounting, avoiding recursive reservations.

## Monitoring

I watch these node-scoped series:

- `cascade_distributed_quota_controller_term` and `cascade_distributed_quota_epoch_resets_total` for ownership stability;
- `cascade_distributed_quota_reservations_total`, `allowed_total`, `throttled_total`, and `rejected_total` on the active controller;
- `cascade_distributed_quota_forwarded_total` on followers;
- `cascade_distributed_quota_configuration_mismatches_total` and `cascade_distributed_quota_failures_total` as fail-closed incident signals;
- the existing per-class `cascade_traffic_quota_*` series for client-visible throttling and rejection.

Principal names never appear in Prometheus labels. I correlate controller CPU, peer request latency, client throttle time, and rejection rate before increasing a quota. This exact coordinator favors safety over disconnected local leasing; I qualify controller throughput on the target deployment before using very small Kafka batches at high request rates.

## Qualification

The deterministic test gate covers codec rejection, pooled capacity, per-principal and per-class isolation, concurrent reservation serialization, rolling activation, mismatched configuration, forwarding over real broker sockets, and controller failover. The commands are in [qualification.md](qualification.md). I still require the authenticated multi-tenant soak and dedicated-host capacity campaign before calling a deployment production qualified.

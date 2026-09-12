# Coordinator completion qualification, 2026-09-12

I qualified the 1.4.0 coordinator implementation at source revision `67dafc6da7c3931d0b1765e7fbb86196c070a1ea` on Windows with Eclipse Adoptium Java 21.0.11 and 12 reported processors. This is correctness and regression evidence from my development machine, not a production SLO or dedicated-host capacity limit.

## What changed

I added self-contained majority decision certificates, voter decision queries, certified cross-node recovery, stale-owner baseline reconciliation, format-3 shard journals, forced atomic checkpoint compaction, compaction and resolver metrics, separate group and delivery mutation monitors, shard-specific readiness, and lifecycle controls. A certified transaction can no longer be contradicted by abort after its original owner disappears.

## Resident-group campaign

I kept 1,000 Kafka consumers resident, used 32 workers, performed one warm-up write and two measured writes per group, stopped the controller, wrote again through the surviving majority, restarted every broker from its existing directory, and verified every final offset.

| Result | Measurement |
| --- | ---: |
| Acknowledged writes | 3,000 / 3,000 |
| Final offsets | 1,000 / 1,000 |
| Coordinator owners used | 1, 2, 3 |
| Controller failover | Passed |
| Full restart recovery | Passed |
| Measured write time | 10.594 s |
| Throughput | 283.180 writes/s |
| p50 / p95 / p99 | 104.998 / 173.315 / 283.373 ms |
| Quorum transactions | 668 committed, 0 failed, 0 rejected |
| Checkpoints | 668 attempts, 0 failures |
| Offset batches | 668 batches, 4,000 requests, 0 failed, 0 rejected |
| Shard journals | 39,152 records, 65,778,835 bytes, 0 torn-tail bytes |

The exact machine-readable result is in [2026-09-12-coordinator-completion.json](2026-09-12-coordinator-completion.json). The quorum peak is one because this workload mutates only the single group service image; the separate group/delivery concurrency test proves the two domains no longer block each other, but I do not infer same-domain linear scaling from it.

## Rolling activation

I pinned format 11 to source `c0ff51721bd74ea7f3f2172e9758a34499ae9a2e` and rotated three real broker processes into 1.4.0. The gate passed rollback before activation, unanimous format-12 activation, downgrade rejection after activation, two-node majority traffic, broker recovery, exact 40/40 consumption, and committed-offset recovery in 10.365 seconds.

The first attempt revealed a real migration defect: one voter held a newer shard baseline than the new owner. The final implementation rejects that stale prepare, queries durable voter images, monotonically reconciles the baseline, and retries without overwriting the newer shard. The clean result is in [2026-09-12-rolling-format11.json](2026-09-12-rolling-format11.json).

## Complete regression

The final whole-software run passed **539/539** tests with no failures or skips in **153 seconds** on Eclipse Adoptium Java 21.0.11. An earlier run exposed a cross-domain adapter defect: a transaction checkpoint included its delivery shards but silently dropped its staged group-offset shard after coordinator failover. The callback now preserves the explicit combined checkpoint operation. The focused delivery and Kafka-client end-to-end rerun passed 33/33 before I repeated the complete suite.

## Limits

I have not run a 72-hour authenticated soak, dedicated Linux RF=3 capacity benchmark, high-cardinality membership rebalance campaign, high-cardinality transactional churn campaign, arbitrary packet-impairment campaign, or physical power/device-loss cut for this revision. Each group image and each delivery image still serializes its own mutations. Those remain production-release gates.

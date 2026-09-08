# Consumer group administration qualification — 2026-09-08

I completed the first Kafka-compatible consumer-group administration milestone at source revision `61c7010ab86497c596dc1440913cb657a92f5b0c`. `ListGroups` v0-v4, `DescribeGroups` v0-v4, and `DeleteGroups` v0-v1 now pass byte-level, real Kafka 4.3.1 Admin, ACL, restart, and coordinator-failover tests. The complete Scala regression passed **498/498**.

## What I qualified

- List and describe use one immutable acknowledged group view and do not expose a staged checkpoint.
- Classic, consumer-protocol, and offset-only groups have deterministic administrative summaries.
- ListGroups v4 filters by state and only reports groups owned by the broker receiving the request.
- Describe returns static instance IDs and authorized-operation bits where the version supports them.
- Describe and Delete enforce per-group ACLs; listing hides unauthorized groups.
- Delete rejects active classic and consumer-protocol groups, reports missing groups, and removes an empty group with all of its offsets in one checkpoint.
- A deleted group and its offsets stay absent after broker restart.
- Kafka Admin can list, describe, and delete through coordinator failover, then receives `GROUP_ID_NOT_FOUND` for the deleted group.

The authenticated Admin test found one real defect before the final pass. A denied coordinator discovery closed the connection, causing Kafka Admin to retry until timeout. Coordinator discovery now returns `GROUP_AUTHORIZATION_FAILED` or `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` in its normal Kafka response. The ACL end-to-end test passes with the corrected behavior.

## Whole-software checks

| Gate | Result |
| --- | --- |
| Scala unit, integration, end-to-end, fault, security, and qualification suite | **498/498 passed** in 189 seconds |
| Kafka 4.3.1 Admin group lifecycle | list, describe, active-delete rejection, empty delete, restart, and failover passed |
| Authenticated ACL scenario | authorized filtering/describe/delete and denied describe/delete passed |
| Staged external client matrix | Java, KafkaJS, Python, franz-go, and .NET each verified **25/25** records; Java recovered **25/25** after restart |
| Staged distribution | three runtime jars staged successfully |
| Image security policy tests | **32/32 passed**, including fail-closed scanner and zero-finding rules |
| Dockerfile validation | passed with no warnings |
| Compose, cluster Compose, Kubernetes, and Grafana | both Compose files rendered, Kubernetes rendered 1,119 lines, dashboard JSON parsed |
| Documentation links | all local links across 35 tracked Markdown files resolved |

The normal Go dependency path received HTTP 403 responses from `proxy.golang.org`. I used the documented fallback: built the exact pinned `compatibility/go` module through `GOPROXY=direct`, producing SHA-256 `adefe87f6cc2d251fd181353da20f83c326f2741d64b357e07e8ad9152e0c1c`, then ran that binary inside the broker's network namespace. This preserved the Go client test; it was not skipped.

I did not build, scan, tag, or publish a new Cascade image in this milestone. The container checks used the staged runtime and pinned JDK image, not a release image.

## 1,000-group coordinator campaign

I reran the persistent 1,000-group, 32-client campaign with two timed rounds, 64-request offset batches, and two-millisecond offset/publication linger. It committed 3,000 timed writes, used coordinator owners 1, 2, and 3, stopped the controller, verified through failover, restarted every broker from its existing disk, and recovered **1,000/1,000** exact final offsets. There were no connection, offset-batch, or publication-queue admission rejections.

| Measurement | Result |
| --- | ---: |
| Timed write throughput | 68.060 writes/s |
| Commit latency p50 / p95 / p99 | 183.430 / 1,441.512 / 3,503.028 ms |
| Coordinator checkpoints / failed attempts | 1,575 / 172 |
| Offset batch requests / batches / failed attempts | 5,386 / 1,784 / 1,386 |
| Publication requests / batches / conflicts | 1,577 / 1,089 / 174 |
| Publication peak retained work | 4 requests / 23,778 bytes |
| Acknowledged offset snapshots / returned keys | 3,002 / 3,000 |

The exact machine-readable result is in the [JSON report](2026-09-08-group-administration.json).

## Result and boundary

The implemented group administration behavior is good for this milestone: every functional, security, failure, recovery, deployment, and compatibility gate I ran passed. The old-or-new acknowledgement boundary held, destructive deletion stayed conservative, and no external client caused a broker protocol error.

This is not production-capacity evidence. The 3.503-second p99, 172 failed checkpoint attempts, 1,386 failed batched attempts before Kafka retries, and 174 publication conflicts show that coordinator contention remains material on this shared development host. The campaign stresses the underlying offset coordinator, not a sustained high-rate Admin API workload. Later consumer-protocol versions, `ConsumerGroupDescribe`, offset administration APIs, high-cardinality rebalance/admin churn, dedicated-host RF=3 capacity, independent shard consensus, and finer-grained mutation locks remain open in the [production-readiness checklist](../production-readiness.md).

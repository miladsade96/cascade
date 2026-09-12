# Coordinator architecture and capacity

I moved coordinator writes off the metadata-controller log. Once every voter supports metadata format 12, Cascade activates `independent-coordinator` and commits group, offset, producer, and transaction changes through the coordinator shard quorum.

## Layout

I separate group IDs and transactional IDs before hashing them. The same text in the two namespaces can therefore have different owners and can never collide accidentally. Rendezvous hashing assigns each typed key to an available voter. The state itself is split into 64 group shards, 64 transaction shards, and one producer-ID allocator shard.

Every touched shard has its own forced journal under `.cascade/coordinator-quorum/shard-NNN.log`. Frames carry a version, length bound, CRC32C, transaction ID, phase, and—only for prepare—the complete atomic delta. Startup truncates an incomplete final frame but rejects corruption inside the durable prefix.

One coordinator transaction follows this order:

1. Lock touched shard IDs in sorted order and enter the bounded transaction gate.
2. Force `prepare` on a voter majority.
3. Force a decision vote on a voter majority.
4. Build a certificate containing the voting node and directory identities, then force that certificate on a voter majority.
5. Force `finalize` remotely, then finalize and publish on the owner.
6. Acknowledge only when the owner and a voter majority reached the terminal record.

The owner stays on its previous acknowledged read image while finalization is blocked. Failed prepare or vote rounds return a retriable coordinator error. Once any valid certificate exists, abort is forbidden: the background resolver queries voter decision state, distributes the certified delta with `recover`, and finishes it on a quorum. A certificate is accepted only when its identities prove a majority of the transaction's voter directory.

If an owner proposes from an older shard version during activation or failover, the prepare round fails closed. The owner then queries durable voter images, monotonically joins the newer shards, installs that baseline, and lets the Kafka client retry. This repaired the format-11-to-format-12 activation boundary without weakening conflict validation.

Independently advancing images are merged by shard version. Equal shard versions must have identical bytes. Readiness tracks each installed shard independently, so equal aggregate versions cannot hide a lagging service image. Coalesced installers resolve the newest image only after taking the group or delivery monitor, which prevents an older callback from overwriting a newly acknowledged local change.

Group and delivery mutations now have separate monitors and separate checkpoint adapters. An ordinary checkpoint publishes only shards that changed between that service's staged and acknowledged images, so a concurrent mutation in the other domain cannot leak into its transaction. A committed Kafka transaction with pending consumer offsets uses an explicit combined checkpoint and publishes its delivery and group-offset shards in the same certified quorum transaction. The delivery adapter preserves that combined operation instead of collapsing it into a delivery-only callback. Each mutable service image still serializes its own changes; the shard quorum itself admits disjoint transactions concurrently.

## Bounded recovery storage

Every journal uses format 3 records but continues to read formats 1 and 2. After a terminal transaction pushes a shard over `--coordinator-journal-compaction-bytes`, Cascade writes one checkpoint plus every unresolved prepare, vote, or certificate record to a sibling file, forces it, atomically replaces the journal, and forces the directory where the platform supports it. Startup installs independent checkpoints monotonically before replaying unresolved transactions. Metrics expose compaction count, checkpoint bytes, reclaimed bytes, pending transactions, resolver runs, recovered transactions, and safe recovery aborts.

## Capacity controls

I bound concurrent coordinator transactions before allocating replication work:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `--coordinator-quorum-max-inflight` | `256` | Maximum admitted shard transactions per broker |
| `--coordinator-quorum-admission-timeout-ms` | `5000` | Maximum wait for an admission permit |
| `--coordinator-resolution-interval-ms` | `1000` | Background scan interval for in-doubt transactions |
| `--coordinator-resolution-delay-ms` | `5000` | Minimum age before another owner resolves a transaction |
| `--coordinator-journal-compaction-bytes` | `67108864` | Per-shard checkpoint replacement threshold |

Overlapping shard sets serialize. Disjoint shard sets can enter quorum replication concurrently. The peer calls use virtual threads, but the voter set remains the controller voter set; this implementation does not create a separate replica assignment for every coordinator shard.

I export attempts, commits, failures, admission rejection, current and peak in-flight work, phase message counts, phase time, encoded record bytes, pending local transactions, journal records/bytes, force time, and torn-tail truncation through the broker snapshot and Prometheus endpoint. I treat any admission rejection, failed coordinator transaction, pending transaction that does not clear, or journal growth outside its planned storage budget as an operational signal.

## Qualification result

On 2026-09-12 I ran 1,000 simultaneously resident Kafka consumers with concurrency 32 and two measured rounds. The run completed 3,000 acknowledged offset writes, verified 1,000/1,000 final offsets, used all three owners, survived controller loss, and recovered after a full restart.

The measured write phase took 10.594 seconds: 283.180 writes/s, 104.998 ms p50, 173.315 ms p95, and 283.373 ms p99. All 668 admitted quorum transactions committed; none failed or were rejected. The three brokers forced 39,152 shard-journal records totaling 65,778,835 bytes, with zero torn-tail truncation. The pinned format-11 campaign also completed all ten phases and exact 40/40 recovery after its stale-owner case exercised baseline reconciliation. The final whole-software run passed 539/539 tests in 153 seconds, including Kafka transaction-plus-offset recovery after coordinator failover. This local Windows result is a correctness and regression measurement, not a production SLO or a dedicated-host capacity limit. The [dated report](performance/2026-09-12-coordinator-completion.md) retains the complete evidence and limitations.

## What I still do not claim

This milestone closes the coordinator architecture items for certified recovery, bounded journals, cross-domain mutation isolation, shard readiness, activation reconciliation, and the format-12 source rolling gate. It does not replace external production qualification:

- Each group image and each delivery image still serializes its own mutations. Removing those two remaining domain monitors requires a deeper partitioned state-store design and separate capacity evidence.
- I still need dedicated Linux hardware campaigns for high-cardinality membership churn, transactional churn, long pauses, controller/voter changes, and multi-day steady state.
- I still need physical device-loss and power-cut evidence. Process-kill and torn-tail tests cannot substitute for that gate.

Until those gates close, I present this as completed coordinator architecture with development-host qualification, not as proof that Cascade has Kafka-equivalent coordinator capacity.

## Reproduce it

```powershell
& 'C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot\bin\java.exe' -jar .tools\sbt-launch-1.12.6.jar "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --batch-max-requests 64 --batch-linger-ms 2 --publication-max-requests 64 --publication-linger-ms 2 --report artifacts/coordinator-completion-1.4.0.json"
```

The report is written only after exact verification succeeds. The `artifacts` directory is intentionally ignored because qualification output belongs to the machine and revision that produced it; I copy the accepted result into a dated tracked report.

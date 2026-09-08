# Coordinator architecture and capacity

I moved coordinator writes off the metadata-controller log. Once every voter supports metadata format 12, Cascade activates `independent-coordinator` and commits group, offset, producer, and transaction changes through the coordinator shard quorum.

## Layout

I separate group IDs and transactional IDs before hashing them. The same text in the two namespaces can therefore have different owners and can never collide accidentally. Rendezvous hashing assigns each typed key to an available voter. The state itself is split into 64 group shards, 64 transaction shards, and one producer-ID allocator shard.

Every touched shard has its own forced journal under `.cascade/coordinator-quorum/shard-NNN.log`. Frames carry a version, length bound, CRC32C, transaction ID, phase, and—only for prepare—the complete atomic delta. Startup truncates an incomplete final frame but rejects corruption inside the durable prefix.

One coordinator transaction follows this order:

1. Lock touched shard IDs in sorted order and enter the bounded transaction gate.
2. Force `prepare` on a voter majority.
3. Force the commit decision on a voter majority.
4. Force `finalize` remotely, then finalize and publish on the owner.
5. Acknowledge only when the owner and a voter majority reached the terminal record.

The owner stays on its previous acknowledged read image while finalization is blocked. Failed prepare or decision rounds return a retriable coordinator error, and a multi-shard change is installed only after every local shard journal contains its terminal marker.

Independently advancing images are merged by shard version. Equal shard versions must have identical bytes. This matters when two owners finish disjoint changes with the same aggregate image version: coalescing cannot discard either change.

## Capacity controls

I bound concurrent coordinator transactions before allocating replication work:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `--coordinator-quorum-max-inflight` | `256` | Maximum admitted shard transactions per broker |
| `--coordinator-quorum-admission-timeout-ms` | `5000` | Maximum wait for an admission permit |

Overlapping shard sets serialize. Disjoint shard sets can enter quorum replication concurrently. The peer calls use virtual threads, but the voter set remains the controller voter set; this implementation does not create a separate replica assignment for every coordinator shard.

I export attempts, commits, failures, admission rejection, current and peak in-flight work, phase message counts, phase time, encoded record bytes, pending local transactions, journal records/bytes, force time, and torn-tail truncation through the broker snapshot and Prometheus endpoint. I treat any admission rejection, failed coordinator transaction, pending transaction that does not clear, or journal growth outside its planned storage budget as an operational signal.

## Qualification result

On 2026-09-08 I ran 1,000 simultaneously resident Kafka consumers with concurrency 32 and two measured rounds. The run completed 3,000 acknowledged offset writes, verified 1,000/1,000 final offsets, used all three owners, survived controller loss, and recovered after a full restart.

The measured write phase took 10.993 seconds: 272.890 writes/s, 102.145 ms p50, 225.710 ms p95, and 413.343 ms p99. All 666 admitted quorum transactions committed; none failed or were rejected. The three brokers forced 28,998 shard-journal records totaling 63,770,586 bytes, with zero torn-tail truncation. This local Windows result is a correctness and regression measurement, not a production SLO or a dedicated-host capacity limit.

## What I still do not claim

This milestone removes coordinator mutations from the metadata log and proves independent shard durability, conflict isolation, bounded admission, restart recovery, failover, and read visibility. It does not finish all coordinator production gates:

- Group and delivery mutations still share a broker-local service lock, so the end-to-end run reached a quorum peak of one even though the quorum layer itself proves concurrent disjoint-shard progress.
- A coordinator process loss between the majority decision and terminal finalization still needs a distributed decision-query and resolution loop. Local journals retain the prepared/decided transaction, but I do not call that a complete cross-node recovery protocol.
- Independent shard journals still need checkpoint compaction and a tested disk-budget policy for indefinite churn.
- I still need dedicated Linux hardware campaigns for high-cardinality membership churn, transactional churn, long pauses, controller/voter changes, and multi-day steady state.

Until those gates close, I present this as the independent coordinator foundation, not as proof that Cascade has Kafka-equivalent coordinator capacity.

## Reproduce it

```powershell
& 'C:\Program Files\Eclipse Adoptium\jdk-21.0.11.10-hotspot\bin\java.exe' -jar .tools\sbt-launch-1.12.6.jar "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 32 --rounds 2 --client-lifecycle persistent --batch-max-requests 64 --batch-linger-ms 2 --publication-max-requests 64 --publication-linger-ms 2 --report artifacts/coordinator-independent.json"
```

The report is written only after exact verification succeeds. The `artifacts` directory is intentionally ignored because qualification output belongs to the machine and revision that produced it.

# Cascade

<p align="center">
  <img src="docs/assets/cascade-logo.png" alt="Cascade logo: high-speed cascading data streams" width="960">
</p>

> I'm building a fast Kafka-style streaming log in pure Scala 3.

I built Cascade around the Kafka wire protocol so existing Kafka clients can connect without a custom SDK. If a language has a client that speaks one of the supported protocol versions, it can talk to Cascade.

The broker itself only needs Scala and the JDK. I use Apache Kafka's Java client in the test suite as an independent compatibility check; it isn't a runtime dependency.

So far, I've implemented broker-assigned offsets, magic-v2 record batches, consumer coordination, durable metadata and offset journals, idempotent producer recovery, transactions, `read_committed` isolation, ISR replication, partition-leader promotion, quorum controller election, online partition reassignment, and dynamic broker/voter membership.

> [!IMPORTANT]
> Cascade isn't a production Kafka replacement yet. I've implemented and tested the single-node delivery semantics and a dynamically reconfigurable cluster with controller failover. Voter changes use durable joint consensus, new observers synchronize before admission, and removing the active controller hands leadership to the surviving quorum. Coordinator-state replication and the other production gates below are still missing.

## Performance I measured

I measured this on my Windows development machine with Java 21, eight partitions, four producers, four consumers, 1 KiB deterministic incompressible payloads, LZ4, `acks=all`, and periodic background flushing:

| Workload | Produce | Consume | Exact verification |
| --- | ---: | ---: | ---: |
| 10,000,000 records, sustained | **182,285 records/s** / **178.0 MiB/s** | **473,058 records/s** | **10,000,000 / 10,000,000** |
| 1,000,000 records, calibration | **614,413 records/s** / **600.0 MiB/s** | **556,232 records/s** / **543.2 MiB/s** | **1,000,000 / 1,000,000** |

After I fixed the background-flush path, sustained ten-million-record production went from 57,400 to 182,285 records/s: a **3.18x improvement**. The write phase dropped from 174.2 to 54.9 seconds. That run forced 9.58 GiB in 47.6 cumulative seconds, so the drive was the main limit on this machine.

The one-million test is much shorter and benefits a lot from the filesystem cache. I don't present either result as production capacity. The [full heavy-load report](docs/performance/2026-08-05-heavy-load.md) includes the machine, workload, latency, CPU, GC, heap, storage, and test method.

## What I'm building

| Area | What is implemented |
| --- | --- |
| Language-neutral access | Length-prefixed Kafka TCP frames and an explicit `ApiVersions` contract; no custom client library required |
| Pure Scala/JDK runtime | Scala 3 broker implementation with Java 21 virtual threads and positional file I/O |
| Efficient record path | Kafka magic-v2 batches remain compressed and opaque; the broker updates only the base offset outside the batch CRC region |
| Delivery guarantees | Producer IDs, epoch fencing, bounded duplicate detection, sequence recovery, transactions, timeouts, transactional offsets, and `read_committed` |
| Durable state | CRC32C-protected metadata, consumer-offset, and delivery-state journals with forced commits and corrupt/partial-tail recovery |
| Consumer coordination | Classic join, sync, heartbeat, leave, rebalance, session expiry, and durable committed offsets |
| Dynamic cluster | Durable joint-consensus membership, Kafka Admin add/remove/describe APIs, controller election and fencing, synchronous ISR replication, persisted committed high watermarks, leader promotion, incremental divergent-tail repair, and safe replica re-admission |
| Measured performance | Repeatable one-million and ten-million tests with exact record counting, latency, CPU, GC, heap, storage, and flush metrics |

## What works now

### Kafka-compatible networking

- Persistent, length-delimited, big-endian Kafka TCP frames.
- Hard request-size bounds and version validation before request handling.
- Correlation IDs preserved in every response.
- Ordered processing within a connection and Java 21 virtual-thread isolation between connections.
- Explicitly advertised API keys and versions rather than a broad, unverified compatibility claim.
- Kafka 4.3.1 Admin, Producer, transactional Producer, explicit Consumer, and subscribed Consumer interoperability tests.

Any language can connect if its client speaks one of the supported Kafka protocol versions. For now, my automated end-to-end tests use the Kafka Java client. I still need to add proper client matrices for Python, Go, .NET, and other languages.

### Storage and durability

- Immutable Kafka magic-v2 record batches stored without broker-side decompression or recompression.
- Client compression remains compatible with Snappy, LZ4, Zstd, and gzip because payloads stay opaque to storage.
- Serialized broker offset assignment per partition.
- Batch-aligned Fetch responses and correct first-batch size behavior.
- Size-based log segments and startup index reconstruction by scanning batch headers.
- Incomplete-tail truncation and removal of later segments whose offsets depend on a damaged earlier tail.
- `periodic` background flushing with dirty-age, byte-threshold, and segment-rollover triggers.
- `sync` flushing for strict per-append local persistence.
- Clean shutdown forces all remaining dirty segments.

Kafka acknowledgements and local disk forcing are separate settings. In single-node periodic mode, `acks=1` and `acks=all` acknowledge a local append before the next scheduled force. A process, OS, or power failure before that force can lose those records. In cluster mode, `acks=all` requires the configured minimum ISR and waits for every current ISR member to append before the committed high watermark advances.

### Idempotence and transactions

In single-node mode I support this Kafka delivery-semantics path:

- Durable producer ID allocation and producer-epoch fencing.
- Per-partition sequence validation with wraparound after `Int.MaxValue`.
- A bounded five-batch duplicate window that returns the original offset for a retry.
- O(1) lookup of recent producer batches on the append hot path.
- Producer sequence recovery from persisted record-batch headers after restart.
- Durable active transaction, outcome, range, timeout, and transactional-offset state.
- Transaction commit and abort, including safe coordination with in-flight appends.
- Last stable offsets and `read_committed` filtering of aborted or open transactions.
- Transaction timeouts and automatic abort when a new epoch fences an old owner.
- Applied checkpoints that recover an interrupted transactional offset commit without replaying old transactions over newer offsets.

This gives Cascade the building blocks for exactly-once processing on one broker. I haven't replicated producer and transaction coordinator state in cluster mode, so I'm not claiming cross-broker delivery failover yet.

### Consumer groups

- Classic Kafka group protocol with member ID allocation.
- Join and sync phases, protocol selection, and leader-provided assignments.
- Heartbeats, session expiration, explicit leave, and rebalance initiation.
- Generation/member validation for ordinary offset commits.
- Append-only CRC32C-protected offset journal with one forced commit per multi-partition request.
- Offset recovery across restart and partial/corrupt-tail truncation.
- Transactional offset staging and commit through `TxnOffsetCommit`.

If a client exposes Kafka's newer consumer protocol, set `group.protocol=classic` for now.

### Dynamic replicated cluster

- Majority-committed, monotonic metadata images.
- Durable controller terms and one vote per term in a forced CRC32C journal.
- Majority election with metadata-freshness voting, randomized retry deadlines, and a preferred initial candidate.
- Controller heartbeats and leases that fence isolated or not-yet-synchronized brokers.
- A committed controller-term image that bumps every partition leader epoch after election.
- Dynamic controller discovery in Kafka Metadata and coordinator responses.
- Round-robin replica assignment and configurable default replication factor.
- Partition leaders, leader epochs, replica sets, and ISR state.
- Parallel synchronous leader-to-follower append.
- `min.insync.replicas` admission for `acks=all`.
- Leader-only Fetch/ListOffsets and committed high-watermark visibility.
- Double-buffered, checksum-protected high-watermark checkpoints that never recover beyond the validated log end.
- Failure detection, ISR shrink, and promotion of a surviving replica with a new leader epoch.
- Chained SHA-256 prefix probes that find a returning replica's last verified common batch boundary.
- Bounded, configurable suffix transfer that preserves the shared prefix and replaces only a divergent or missing tail.
- A short final-delta fence so Produce cannot race ISR admission after the online bulk copy.
- ISR re-admission only after catch-up succeeds and the new metadata image reaches quorum.
- Durable online reassignment state with learners outside the ISR, add-before-remove ordering, replacement plans, and cancellation.
- Bulk learner catch-up without blocking Produce, followed by a short final-delta fence and atomic target-assignment commit.
- Kafka Admin `AlterPartitionReassignments` and `ListPartitionReassignments` compatibility, including controller-failover resume.
- Persisted voter endpoints and directory IDs with backward-compatible bootstrap from the configured initial quorum.
- Stable → joint → stable membership changes that require majorities from both competing voter sets for elections, leases, and metadata commits.
- Observer discovery and committed-metadata synchronization before a broker can become a voter.
- Kafka Admin `DescribeMetadataQuorum`, `AddRaftVoter`, and `RemoveRaftVoter` compatibility.
- One membership change at a time, partition-drain enforcement before removal, automatic completion after controller failover, and active-controller handoff.
- Rollback of uncommitted `acks=all` appends so a transient replica miss cannot poison the next retry's base offset.
- Real Kafka-client end-to-end verification across partition-leader loss, controller loss, stale-term rejection, metadata creation after election, and broker restart/rejoin.

The configured node list bootstraps the first committed voter set and gives observers discovery endpoints. After that, the committed metadata image is authoritative. Recovery is incremental at Kafka batch boundaries; only its final delta and ISR admission are partition-fenced. Group, offset, producer, and transaction coordinator state is still local to one broker, so those services don't have production failover yet.

## Run Cascade

### Requirements

- JDK 21+
- Network access to Maven Central for the first build

The included launchers download SBT the first time you run them.

### Run the tests and start one broker

Windows:

```powershell
.\sbt.bat test
.\sbt.bat "run --host 0.0.0.0 --port 9092 --advertised-host localhost --data-dir data"
```

Linux/macOS:

```bash
chmod +x sbt
./sbt test
./sbt "run --host 0.0.0.0 --port 9092 --advertised-host localhost --data-dir data"
```

Point any supported Kafka client at `localhost:9092`. A basic single-node producer configuration is:

```properties
bootstrap.servers=localhost:9092
acks=all
enable.idempotence=true
```

For a consumer using subscriptions and committed-only visibility:

```properties
bootstrap.servers=localhost:9092
group.protocol=classic
isolation.level=read_committed
```

Set `transactional.id` on a producer when you want to use transactions. Use `read_uncommitted` only when the consumer should also see aborted transactional records.

### Bootstrap a three-node cluster for development

Run each command in a separate process and give every broker its own data directory:

```powershell
.\sbt.bat "run --host 127.0.0.1 --port 9092 --advertised-host 127.0.0.1 --advertised-port 9092 --node-id 1 --data-dir data-1 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9093 --advertised-host 127.0.0.1 --advertised-port 9093 --node-id 2 --data-dir data-2 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9094 --advertised-host 127.0.0.1 --advertised-port 9094 --node-id 3 --data-dir data-3 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
```

For now, keep `enable.idempotence=false` in cluster tests. I haven't replicated producer and transaction coordinator state yet.

### Add or remove a voter online

I start a new node as an observer by pointing `--cluster-nodes` at the existing bootstrap voters while giving it a new `--node-id`, listener, and data directory. The local node does not need to appear in that bootstrap list. Then I use the standard Kafka 4.3 Admin API:

```java
Uuid directoryId = Uuid.randomUuid();
admin.addRaftVoter(
    4,
    directoryId,
    Set.of(new RaftVoterEndpoint("CONTROLLER", "127.0.0.1", 9095))
).all().get();
```

I keep that directory ID with the node identity. `describeMetadataQuorum()` also reports the committed voter identities and endpoints. Before removing a voter, I reassign every partition replica away from it; Cascade rejects removal while any assignment still references that broker. Then I call `removeRaftVoter(nodeId, directoryId)`. If that node is the active controller, Cascade commits the new voter set before stepping down.

## Kafka wire compatibility

Cascade returns exactly this matrix from `ApiVersions`:

| API | Key | Versions | Implemented behavior |
| --- | ---: | ---: | --- |
| Produce | 0 | 3 | `acks` 0/1/all, idempotent sequence validation/deduplication, transactional batches |
| Fetch | 1 | 6 | `read_uncommitted`/`read_committed`, high watermark, last stable offset, batch-aligned limits |
| ListOffsets | 2 | 2 | Isolation-aware earliest (`-2`) and latest (`-1`) offsets |
| Metadata | 3 | 4 | Broker/topic/partition discovery and optional auto-creation |
| OffsetCommit | 8 | 7 | Generation validation and durable multi-partition commits |
| OffsetFetch | 9 | 5 | Requested or all committed group offsets |
| FindCoordinator | 10 | 2 | Classic group and transaction coordinator discovery |
| JoinGroup | 11 | 5 | Classic membership, protocol selection, and leader election |
| Heartbeat | 12 | 3 | Session liveness and generation validation |
| LeaveGroup | 13 | 2 | Explicit departure and rebalance initiation |
| SyncGroup | 14 | 3 | Leader assignments and follower synchronization |
| ApiVersions | 18 | 0-4 | Legacy and flexible encodings with tagged fields |
| CreateTopics | 19 | 2 | Validation and quorum-committed topic metadata |
| InitProducerId | 22 | 1 | Durable producer IDs, epoch allocation, and fencing |
| AddPartitionsToTxn | 24 | 1 | Transaction partition enrollment and timeout start |
| AddOffsetsToTxn | 25 | 1 | Consumer-group enrollment in a transaction |
| EndTxn | 26 | 1 | Durable commit/abort outcome and offset-application checkpoint |
| TxnOffsetCommit | 28 | 2 | Staged offsets made visible only by transaction commit |
| AlterPartitionReassignments | 45 | 0 | Start, replace, or cancel a durable online replica move |
| ListPartitionReassignments | 46 | 0 | Report intermediate, adding, and removing replicas |
| DescribeQuorum | 55 | 0-2 | Metadata leader, epoch, high watermark, voter identities, and endpoints |
| AddRaftVoter | 80 | 0-1 | Synchronize an observer and commit a durable joint-consensus admission |
| RemoveRaftVoter | 81 | 0 | Validate identity and partition drain, then remove and hand off leadership if needed |

I follow the [Apache Kafka 4.3 protocol grammar](https://kafka.apache.org/43/design/protocol/). Cascade only advertises and accepts the versions listed above; I don't want to claim support for versions I haven't tested.

## Architecture

```text
Kafka client in any language
        |
        | Kafka length-delimited TCP frames
        v
Virtual-thread connection handler
        |
        | frame bounds + versioned binary codec
        v
Request routing
        |
        +-- metadata quorum + durable joint membership, election, and fencing
        +-- classic group coordinator + durable offset journal
        +-- producer/transaction coordinator + delivery journal
        +-- partition log
              +-- opaque Kafka magic-v2 batches
              +-- broker-assigned offsets
              +-- bounded producer-state index
              +-- segment rollover and crash-tail recovery
              +-- background or synchronous flushing
              +-- ISR follower replication and committed watermark
```

I keep records serialized on the data path. Positional file I/O avoids shared channel-position races, each partition serializes offset assignment, and Fetch returns complete record batches. A transactional append reserves its range before storage. That stops `EndTxn` from racing an in-flight append and keeps the last stable offset behind open work.

## Heavy-load test details

### Ten-million sustained workload

```text
records             10,000,000
payload             1,024 bytes, deterministic incompressible variants
partitions          8
producer clients    4
consumer clients    4
compression         LZ4
acks                all
batch.size          128 KiB
linger.ms           5
segment size        256 MiB
flush policy        periodic
flush interval      1,000 ms
flush byte limit    64 MiB per partition
```

| Metric | Result |
| --- | ---: |
| Produce throughput | **182,285 records/s / 178.0 MiB/s** |
| Produce elapsed | **54.859 s** |
| Produce CPU | 1.75 cores |
| Ack latency p50 | <=1,000 ms |
| Ack latency p95 | >5,000 ms |
| Ack latency max | 19,017.787 ms |
| Force operations during produce | 131 |
| Data explicitly forced | 9,805.2 MiB |
| Cumulative force time | 47,624.5 ms |
| Consume throughput | **473,058 records/s** |
| Consume elapsed | **21.139 s** |
| Consumer verification | **10,000,000 / 10,000,000 passed** |
| Peak shared-JVM heap | 5,221.3 MiB |

I start the acknowledgement timer when each asynchronous `send` is offered, so saturation latency includes producer-side queueing. The producers offered data faster than my local temporary drive could force it during a sustained ten-gigabyte run. That's why p95 stayed above five seconds after the flush fix.

### One-million calibration

| Metric | Result |
| --- | ---: |
| Produce throughput | **614,413 records/s / 600.0 MiB/s** |
| Produce elapsed | 1.628 s |
| Ack latency max | 449.623 ms |
| Force operations during produce | 8 |
| Consume throughput | **556,232 records/s / 543.2 MiB/s** |
| Consumer verification | **1,000,000 / 1,000,000 passed** |
| Peak shared-JVM heap | 1,259.0 MiB |

After adding the cluster path, I ran this test again. It reached 615,592 produced and 527,001 consumed records/s, with p99 acknowledgement latency at or below 500 ms and a 422.632 ms maximum. I didn't find a meaningful single-node regression, but this short cache-assisted test isn't a sustained storage benchmark.

After adding dynamic membership and retry rollback, I ran the same one-million-record regression on 2026-08-17. It verified exactly **1,000,000 / 1,000,000** records at **367,202 produced records/s** and **250,135 consumed records/s**. The run used 7.43 producer-side CPU cores on my shared development machine, so I treat it as a correctness and regression signal rather than a new capacity claim.

### Reproduce the load test

```bash
./sbt "Test/runMain cascade.performance.LoadTest --records 10000000 --payload-bytes 1024 --partitions 8 --producers 4 --consumers 4 --compression lz4 --flush-policy periodic --flush-interval-ms 1000 --flush-bytes 67108864"
```

On Windows, replace `./sbt` with `.\sbt.bat`. Add `--keep-data` if you want to keep the generated segment directory. I leave this harness out of normal `sbt test` runs because it's large and its result depends on the machine.

The harness reports:

- Produce and consume records/s and payload MiB/s.
- Acknowledgement p50, p95, p99, p99.9, and maximum latency.
- Process CPU, machine CPU, GC collections, and GC time.
- Stored bytes, bytes per record, force count/volume/time, and pending dirty bytes.
- Peak heap and exact consumed-record verification.

The [complete 2026-08-05 report](docs/performance/2026-08-05-heavy-load.md) compares this result with the old per-request-force implementation and explains what changed.

## Verification

The current clean `sbt test` suite passes **71/71 tests** in three layers:

- Unit tests for binary codecs, bounds failures, record-batch metadata and sequence wrap, storage pagination, segment rollover, flush policies, corrupt/partial-tail recovery, durable high-watermark recovery, incremental suffix truncation, recovery fingerprints, delivery-state recovery, producer fencing, transaction timeout, interrupted offset application, group coordination, metadata recovery, and durable controller term/vote recovery.
- TCP integration tests for discovery, Produce/Fetch, acknowledgement behavior, duplicate retry offsets, sequence-gap rejection, idempotent state recovery after broker restart, flexible voter-change framing, and Kafka quorum description decoding.
- Kafka 4.3.1 end-to-end tests for Admin/Producer/Consumer interoperability, classic group rebalances, committed offsets across restart, RF=3/RF=4 replication, ISR/leader failover, controller election and stale-term fencing, divergent-tail replacement, safe replica re-admission, reassignment visibility/cancellation, reassignment through controller loss, live voter admission, active-controller removal and restart as an observer, transactions, commit/abort isolation, active last stable offsets, and transactional consumer offsets.

The load harness separately checks the exact record count at one million and ten million records.

## What I plan to build next

| Priority | Area | Planned work |
| ---: | --- | --- |
| 1 | Coordinator failover | Replicated classic-group, consumer-offset, producer, and transaction coordinator state across brokers |
| 2 | Availability qualification | Automated crash, kill, power-loss, and network-partition testing across stable and joint voter configurations |
| 3 | Storage lifecycle | Time/size retention, log compaction, offset compaction/expiry, timestamp and transaction indexes, disk-pressure handling, and safe deletion |
| 4 | Security and isolation | TLS, SASL mechanisms, ACL authorization, audit events, secret rotation, quotas, bounded queues, overload shedding, and connection/request limits |
| 5 | Operations | Metrics export, health/readiness endpoints, structured logs, admin API coverage, backup/restore, capacity alerts, and operational runbooks |
| 6 | Compatibility | New Kafka consumer protocol, static-member fencing, more API versions, malformed-frame/fuzz testing, multiple client languages, and rolling upgrade/downgrade testing |
| 7 | Qualification | Multi-day soak, kill/crash/power-loss simulation, network partitions, disk-full/corruption testing, and dedicated-host replicated-cluster benchmarks |
| 8 | Profile-driven optimization | Zero-copy Fetch with `FileChannel.transferTo`, selector/worker pools, multi-device log placement, and further changes justified by profiling |

I track the release gates in [docs/production-readiness.md](docs/production-readiness.md). I won't call Cascade a production Kafka replacement until every blocking gate passes on the deployment topology I document.

## What is still missing

- Dynamic membership is implemented, but automatic broker registration, rolling version negotiation, and exhaustive crash/power-loss testing during each joint phase are not complete.
- Replica recovery and reassignment transfer bounded record-batch chunks rather than zero-copy segment files; Produce is briefly fenced for the final delta and metadata transition.
- Group, offset, producer, and transaction coordinator state is not replicated for failover.
- Retention, compaction, timestamp indexes, quotas, TLS/SASL, ACLs, and operational endpoints are not implemented.
- Only the classic consumer group protocol is supported.
- The automated client matrix currently uses Kafka Java client 4.3.1.
- The performance figures are single-node, shared-JVM development-machine measurements; replicated-cluster capacity has not been benchmarked.

## Configuration

| Option | Default | Meaning |
| --- | --- | --- |
| `--host` | `0.0.0.0` | Listener bind host |
| `--port` | `9092` | Listener port; `0` selects a free test port |
| `--advertised-host` | `localhost` | Host returned by Metadata |
| `--advertised-port` | Listener port | Port returned by Metadata |
| `--data-dir` | `data` | Topic segments and internal journal root |
| `--max-request-bytes` | `104857600` | Hard request-frame bound |
| `--segment-bytes` | `134217728` | Segment rollover target |
| `--flush-policy` | `periodic` | Batched background forcing or strict per-append `sync` |
| `--flush-interval-ms` | `1000` | Maximum periodic dirty age |
| `--flush-bytes` | `67108864` | Per-partition dirty-byte threshold that schedules a force |
| `--node-id` | `1` | Broker/controller ID |
| `--cluster-nodes` | Empty | Initial voters or observer discovery endpoints as `id@host:port`; empty selects single-node mode |
| `--controller-id` | `1` | Preferred initial controller candidate; any configured voter can be elected later |
| `--default-replication-factor` | `1` | Replication factor used for auto-created topics |
| `--min-insync-replicas` | `1` | Minimum ISR required by `acks=all` |
| `--peer-timeout-ms` | `3000` | Internal metadata and replica RPC timeout |
| `--replica-recovery-timeout-ms` | `300000` | Maximum controller wait for one replica recovery operation |
| `--replica-recovery-chunk-bytes` | `8388608` | Maximum record-batch payload requested per incremental recovery transfer |
| `--controller-heartbeat-ms` | `250` | Elected-controller heartbeat interval |
| `--controller-election-timeout-ms` | `1500` | Controller lease and minimum election timeout; must be at least three heartbeat intervals |
| `--no-auto-create` | Off | Disable Metadata/Produce auto-creation |

## More documentation

- [Production-readiness gates](docs/production-readiness.md)
- [Heavy-load report](docs/performance/2026-08-05-heavy-load.md)
- [Contributing](CONTRIBUTING.md)
- [Apache-2.0 license](LICENSE)

## License

Apache License 2.0.

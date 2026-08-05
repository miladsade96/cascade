# Cascade

> A pure Scala 3 streaming log with Kafka wire-protocol compatibility, durable storage, idempotent and transactional delivery, classic consumer groups, and a tested static replication path.

Cascade is built so existing Kafka-protocol clients can connect without a Cascade-specific SDK. The broker runtime uses only Scala and the JDK; Apache Kafka's Java client is a test-scoped compatibility oracle, not a runtime dependency.

Cascade already demonstrates the core mechanics of a Kafka-style system, including broker-assigned offsets, magic-v2 record batches, consumer coordination, durable metadata and offset journals, idempotent producer recovery, transactions, `read_committed` isolation, ISR replication, and leader promotion.

> [!IMPORTANT]
> Cascade is an advanced development broker, not yet a production replacement for an Apache Kafka cluster. Single-node delivery semantics are implemented and tested. The three-node mode is a static replication milestone and still lacks coordinator replication, controller election, replica catch-up, and other production gates documented below.

## Performance at a glance

Measured on the documented Windows development machine with Java 21, eight partitions, four producers, four consumers, 1 KiB deterministic incompressible payloads, LZ4, `acks=all`, and periodic background flushing:

| Workload | Produce | Consume | Exact verification |
| --- | ---: | ---: | ---: |
| 10,000,000 records, sustained | **182,285 records/s** / **178.0 MiB/s** | **473,058 records/s** | **10,000,000 / 10,000,000** |
| 1,000,000 records, calibration | **614,413 records/s** / **600.0 MiB/s** | **556,232 records/s** / **543.2 MiB/s** | **1,000,000 / 1,000,000** |

The corrected background-flush path improved sustained ten-million-record production from 57,400 to 182,285 records/s, a **3.18x increase**, and reduced the write phase from 174.2 to 54.9 seconds. The long run explicitly forced 9.58 GiB in 47.6 cumulative seconds, showing that sustained production was storage-bound on that machine.

The one-million result benefits heavily from the filesystem cache and must not be extrapolated as sustained disk throughput. These are shared-JVM development-machine regression measurements, not production capacity claims. See [the complete heavy-load report](docs/performance/2026-08-05-heavy-load.md) for the environment, latency distribution, CPU, GC, heap, storage, and methodology.

## Why Cascade is interesting

| Capability | What Cascade demonstrates |
| --- | --- |
| Language-neutral access | Length-prefixed Kafka TCP frames and an explicit `ApiVersions` contract; no custom client library required |
| Pure Scala/JDK runtime | Scala 3 broker implementation with Java 21 virtual threads and positional file I/O |
| Efficient record path | Kafka magic-v2 batches remain compressed and opaque; the broker updates only the base offset outside the batch CRC region |
| Delivery guarantees | Producer IDs, epoch fencing, bounded duplicate detection, sequence recovery, transactions, timeouts, transactional offsets, and `read_committed` |
| Durable state | CRC32C-protected metadata, consumer-offset, and delivery-state journals with forced commits and corrupt/partial-tail recovery |
| Consumer coordination | Classic join, sync, heartbeat, leave, rebalance, session expiry, and durable committed offsets |
| Static replication | RF=3 partition assignment, synchronous ISR replication, committed high watermarks, leader epochs, ISR shrink, and surviving-replica promotion |
| Measurable performance | Reproducible one-million and ten-million tests with exact record counting, latency, CPU, GC, heap, storage, and flush metrics |

## Existing features

### Kafka-compatible networking

- Persistent, length-delimited, big-endian Kafka TCP frames.
- Hard request-size bounds and version validation before request handling.
- Correlation IDs preserved in every response.
- Ordered processing within a connection and Java 21 virtual-thread isolation between connections.
- Explicitly advertised API keys and versions rather than a broad, unverified compatibility claim.
- Kafka 4.3.1 Admin, Producer, transactional Producer, explicit Consumer, and subscribed Consumer interoperability tests.

Any language with a client that speaks the supported Kafka protocol versions can connect to Cascade. Automated end-to-end compatibility currently uses the Kafka Java client; broader Python, Go, .NET, and other client matrices remain upcoming qualification work.

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

Kafka acknowledgements and local disk forcing are separate controls. In single-node periodic mode, `acks=1` and `acks=all` acknowledge a local append before the next scheduled force and can be lost after a simultaneous process, OS, or power failure. In cluster mode, `acks=all` requires the configured minimum ISR and waits for every current ISR member to append before advancing the committed high watermark.

### Idempotence and transactions

Single-node mode supports the Kafka delivery-semantics path:

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

This provides the building blocks for exactly-once processing on one broker. Producer and transaction coordinator state is not replicated in the current static-cluster mode, so cross-broker delivery failover is not yet claimed.

### Consumer groups

- Classic Kafka group protocol with member ID allocation.
- Join and sync phases, protocol selection, and leader-provided assignments.
- Heartbeats, session expiration, explicit leave, and rebalance initiation.
- Generation/member validation for ordinary offset commits.
- Append-only CRC32C-protected offset journal with one forced commit per multi-partition request.
- Offset recovery across restart and partial/corrupt-tail truncation.
- Transactional offset staging and commit through `TxnOffsetCommit`.

Clients exposing Kafka's newer consumer protocol should set `group.protocol=classic` until Cascade implements that protocol.

### Static three-node replication

- Majority-committed, monotonic metadata images.
- Round-robin replica assignment and configurable default replication factor.
- Partition leaders, leader epochs, replica sets, and ISR state.
- Parallel synchronous leader-to-follower append.
- `min.insync.replicas` admission for `acks=all`.
- Leader-only Fetch/ListOffsets and committed high-watermark visibility.
- Failure detection, ISR shrink, and promotion of a surviving replica with a new leader epoch.
- Real Kafka-client end-to-end verification before and after the original partition leader stops.

Membership and the controller remain static. Returning replicas cannot catch up or safely rejoin the ISR yet, and the controller/group/delivery coordinators do not have production failover.

## Quick start

### Requirements

- JDK 21+
- Network access to Maven Central for the first build

The checked-in launchers download SBT automatically.

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

Point a Kafka-protocol client at `localhost:9092`. A representative single-node producer configuration is:

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

Set `transactional.id` on a producer to use Kafka transactions. Use `read_uncommitted` when a consumer intentionally needs to inspect aborted transactional records.

### Run a static three-node development cluster

Run each command in a separate process and give every broker its own data directory:

```powershell
.\sbt.bat "run --host 127.0.0.1 --port 9092 --advertised-host 127.0.0.1 --advertised-port 9092 --node-id 1 --data-dir data-1 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9093 --advertised-host 127.0.0.1 --advertised-port 9093 --node-id 2 --data-dir data-2 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9094 --advertised-host 127.0.0.1 --advertised-port 9094 --node-id 3 --data-dir data-3 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
```

Keep `enable.idempotence=false` in static-cluster testing until producer/transaction coordinator state is replicated.

## Kafka wire compatibility

Cascade returns this exact matrix from `ApiVersions`:

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

The implementation follows the [Apache Kafka 4.3 protocol grammar](https://kafka.apache.org/43/design/protocol/), but only the versions above are currently advertised and accepted.

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
        +-- metadata quorum journal + static controller
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

The data path avoids record deserialization. File-channel positional I/O prevents shared channel-position races, partition logs serialize offset assignment, and Fetch returns complete record batches. Transactional appends reserve their range before storage so `EndTxn` cannot race an in-flight append and the last stable offset does not move past open work.

## Heavy-load results

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

The acknowledgement histogram starts when each asynchronous `send` is offered, so saturation latency includes producer-side queueing. Producers offered data faster than the local system-temp device could force it for a sustained ten-gigabyte run, which is why p95 exceeded five seconds even after the flush correction.

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

A later post-cluster regression run remained at 615,592 produced and 527,001 consumed records/s with p99 acknowledgement latency at or below 500 ms and a 422.632 ms maximum. This confirms no material single-node regression, but short cache-assisted runs are not sustained storage benchmarks.

### Reproduce the load test

```bash
./sbt "Test/runMain cascade.performance.LoadTest --records 10000000 --payload-bytes 1024 --partitions 8 --producers 4 --consumers 4 --compression lz4 --flush-policy periodic --flush-interval-ms 1000 --flush-bytes 67108864"
```

On Windows, replace `./sbt` with `.\sbt.bat`. Use `--keep-data` to retain the generated segment directory. The harness is deliberately excluded from ordinary `sbt test` so CI remains deterministic and fast.

The harness reports:

- Produce and consume records/s and payload MiB/s.
- Acknowledgement p50, p95, p99, p99.9, and maximum latency.
- Process CPU, machine CPU, GC collections, and GC time.
- Stored bytes, bytes per record, force count/volume/time, and pending dirty bytes.
- Peak heap and exact consumed-record verification.

See [the complete 2026-08-05 report](docs/performance/2026-08-05-heavy-load.md) for the comparison against the former per-request-force implementation and detailed interpretation.

## Verification

The current `sbt test` suite passes **39/39 tests** across three layers:

- Unit tests for binary codecs, bounds failures, record-batch metadata and sequence wrap, storage pagination, segment rollover, flush policies, corrupt/partial-tail recovery, delivery-state recovery, producer fencing, transaction timeout, interrupted offset application, group coordination, and metadata recovery.
- TCP integration tests for discovery, Produce/Fetch, acknowledgement behavior, duplicate retry offsets, sequence-gap rejection, and idempotent state recovery after broker restart.
- Kafka 4.3.1 end-to-end tests for Admin/Producer/Consumer interoperability, classic group rebalances, committed offsets across restart, RF=3 replication, ISR/leader failover, transactions, commit/abort isolation, active last stable offsets, and transactional consumer offsets.

The load harness separately verifies exact record counts at one-million and ten-million scale.

## Upcoming features

| Priority | Area | Planned work |
| ---: | --- | --- |
| 1 | Availability and replication | Quorum controller election, broker fencing, offline-replica reconciliation, catch-up, safe ISR re-admission, reassignment, and persisted high-watermark recovery |
| 2 | Coordinator failover | Replicated classic-group, consumer-offset, producer, and transaction coordinator state across brokers |
| 3 | Storage lifecycle | Time/size retention, log compaction, offset compaction/expiry, timestamp and transaction indexes, disk-pressure handling, and safe deletion |
| 4 | Security and isolation | TLS, SASL mechanisms, ACL authorization, audit events, secret rotation, quotas, bounded queues, overload shedding, and connection/request limits |
| 5 | Operations | Metrics export, health/readiness endpoints, structured logs, admin API coverage, backup/restore, capacity alerts, and operational runbooks |
| 6 | Compatibility | New Kafka consumer protocol, static-member fencing, more API versions, malformed-frame/fuzz testing, multiple client languages, and rolling upgrade/downgrade testing |
| 7 | Qualification | Multi-day soak, kill/crash/power-loss simulation, network partitions, disk-full/corruption testing, and dedicated-host replicated-cluster benchmarks |
| 8 | Profile-driven optimization | Zero-copy Fetch with `FileChannel.transferTo`, selector/worker pools, multi-device log placement, and further changes justified by profiling |

The measurable release gates live in [docs/production-readiness.md](docs/production-readiness.md). Cascade should not be described as a production Kafka replacement until every blocking gate passes on the documented deployment topology.

## Current limitations

- The controller and broker membership are static; there is no controller election.
- Offline replicas cannot catch up or safely rejoin the ISR automatically.
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
| `--cluster-nodes` | Empty | Static comma-separated voters as `id@host:port`; empty selects single-node mode |
| `--controller-id` | `1` | Fixed metadata and classic-group controller |
| `--default-replication-factor` | `1` | Replication factor used for auto-created topics |
| `--min-insync-replicas` | `1` | Minimum ISR required by `acks=all` |
| `--peer-timeout-ms` | `3000` | Internal metadata and replica RPC timeout |
| `--no-auto-create` | Off | Disable Metadata/Produce auto-creation |

## Additional documentation

- [Production-readiness gates](docs/production-readiness.md)
- [Heavy-load report](docs/performance/2026-08-05-heavy-load.md)
- [Contributing](CONTRIBUTING.md)
- [Apache-2.0 license](LICENSE)

## License

Apache License 2.0.

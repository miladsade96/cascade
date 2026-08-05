# Cascade

Cascade is a Kafka-wire-compatible streaming log implemented in Scala 3. It runs as a single broker by default and now has an explicit static three-node mode with majority-replicated metadata, partition replicas, ISR high watermarks, leader epochs, and surviving-replica promotion. Current Kafka clients can create topics, discover partitions, produce compressed or uncompressed magic-v2 record batches, join classic consumer groups, durably commit offsets, and consume by subscription or explicit partition assignment.

The broker runtime is pure Scala/JDK code. Apache Kafka's Java client is present only in the test scope, where it acts as an independent compatibility oracle.

> Cascade is an early broker implementation, not a drop-in replacement for a production Kafka cluster yet. Its compatibility boundary is explicit below.

## Requirements and commands

- JDK 21+
- Network access to Maven Central on the first build; the checked-in launchers download SBT automatically

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

Any Kafka client can then use `localhost:9092` as its bootstrap server. Configure producers with `enable.idempotence=false`. Consumer subscription is supported through the classic group protocol; set `group.protocol=classic` explicitly on clients that expose the newer consumer protocol so the compatibility choice remains stable across client upgrades.

Static three-node development cluster (run each command in a separate process):

```powershell
.\sbt.bat "run --host 127.0.0.1 --port 9092 --advertised-host 127.0.0.1 --advertised-port 9092 --node-id 1 --data-dir data-1 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9093 --advertised-host 127.0.0.1 --advertised-port 9093 --node-id 2 --data-dir data-2 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9094 --advertised-host 127.0.0.1 --advertised-port 9094 --node-id 3 --data-dir data-3 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
```

This mode is a tested replication milestone, not yet a production deployment topology. Membership and the controller are static; use separate data directories for every node.

## Wire compatibility

Cascade returns this exact matrix from `ApiVersions`:

| API | Key | Versions | Implemented behavior |
| --- | ---: | ---: | --- |
| Produce | 0 | 3 | `acks` 0/1/all, batch append, broker offset assignment |
| Fetch | 1 | 6 | partition reads, high watermark, log start offset, batch-aligned limits |
| ListOffsets | 2 | 2 | earliest (`-2`) and latest (`-1`) offsets |
| Metadata | 3 | 4 | broker/topic/partition discovery and optional auto-creation |
| OffsetCommit | 8 | 7 | generation validation and durable multi-partition commits |
| OffsetFetch | 9 | 5 | requested or all committed group offsets |
| FindCoordinator | 10 | 2 | classic group coordinator discovery; fixed controller in cluster mode |
| JoinGroup | 11 | 5 | classic membership, protocol selection, and leader election |
| Heartbeat | 12 | 3 | session liveness and generation validation |
| LeaveGroup | 13 | 2 | explicit departure and rebalance initiation |
| SyncGroup | 14 | 3 | leader assignments and follower synchronization |
| ApiVersions | 18 | 0–4 | legacy and flexible encodings, tagged fields |
| CreateTopics | 19 | 2 | validation and quorum-replicated topic metadata |

Kafka wire protocol uses length-prefixed, big-endian TCP frames. Cascade bounds every incoming frame, processes requests in connection order, keeps connections persistent, and returns the client's correlation ID unchanged. The implementation follows the [Apache Kafka 4.3 protocol grammar](https://kafka.apache.org/43/design/protocol/).

## Architecture

```text
Kafka client in any language
        │ length-delimited Kafka TCP frames
        ▼
  virtual-thread connection handler
        │ bounds check + versioned codec
        ▼
  immutable request routing
        │
        ├── forced metadata image journal + majority commit
        ├── static controller, failure detector, leader epochs, ISR
        ├── classic group coordinator + offset journal
        └── leader/follower partition append log
                ├── Kafka magic-v2 batches stored verbatim
                ├── broker-assigned base offsets
                ├── parallel follower replication + committed high watermark
                ├── size-based segment rollover
                └── startup index recovery by linear scan
```

The record hot path does not deserialize records or decompress client batches. Cascade changes only the batch base offset—which is outside the record-batch CRC region—and persists the original batch bytes. This keeps Snappy, LZ4, Zstd, and gzip payload compatibility in the client, where compression is already implemented.

Concurrency is deliberately simple: Java 21 virtual threads isolate slow connections, requests remain ordered within each connection, and each partition log serializes offset assignment with its append. File-channel positional I/O prevents shared channel-position races. Fetches are batch aligned, as required by Kafka's record-set semantics.

### Durability and flushing

Kafka acknowledgements and physical disk flushing are separate concerns. In single-node mode, `acks=1` and `acks=all` both acknowledge after the local append. In cluster mode, `acks=all` is rejected when the ISR is below `--min-insync-replicas`, waits for every current ISR member to append, then advances the committed high watermark on the leader and followers. Fetch and ListOffsets are leader-only and expose committed data. `acks=1` can acknowledge data that has not reached the ISR high watermark.

Cluster metadata is stored as monotonic, CRC32C-protected full images. The fixed controller prepares a new image against the configured voters, commits it only after a majority durably accepts it, and recovers the highest image held by a majority before allowing new mutations. Its failure detector removes an unavailable broker from ISR and promotes a surviving replica with an incremented leader epoch.

The default `periodic` policy batches `FileChannel.force(false)` operations across dirty data instead of forcing every Produce request. A broker-wide flusher runs forces outside partition append locks, schedules an immediate pass after 64 MiB becomes dirty or a segment rolls, and otherwise limits the dirty interval to one second. Clean shutdown always flushes remaining data. Use `--flush-policy sync` only when every single-node append must reach stable storage before acknowledgement; it is intentionally much slower.

Periodic acknowledgements can be lost after simultaneous process, OS, or power failures before the next flush. Recovery truncates an incomplete batch tail and, if necessary, discards later segments whose offsets depend on that tail. Replication reduces the single-machine failure domain, but the current cluster mode still lacks offline-replica catch-up, ISR re-admission, persisted high-watermark reconciliation, and controller election.

Committed consumer offsets use a separate append-only, CRC32C-protected journal. Each OffsetCommit request is forced once after all of its partition updates are appended and becomes visible only after that force succeeds. Startup recovery truncates a partial or corrupt tail. Offset retention and journal compaction remain production-readiness work.

## Tests

`sbt test` runs all three layers:

- Unit: primitive/flexible wire codecs, bounds failures, record-batch offsets, replica commit visibility, segment rollover, log recovery, group coordination, metadata-journal recovery, and offset-journal recovery.
- Integration: a real TCP socket sends hand-encoded ApiVersions, Metadata, Produce, and Fetch frames over one persistent connection.
- End to end: Apache Kafka client 4.3.1 creates topics, produces and consumes records, coordinates two subscribed consumers through a rebalance, commits offsets, restarts a broker, and verifies a three-node replicated partition before and after its leader is stopped and replaced by a surviving replica.

### Reproducible heavy-load test

The load harness uses real Kafka clients, durable acknowledgements, explicit partitions, incompressible payload variants, and full consumer record-count verification. It reports producer/consumer throughput, acknowledgement latency buckets, process CPU, GC, peak heap, and storage amplification:

```bash
./sbt "Test/runMain cascade.performance.LoadTest --records 1000000 --payload-bytes 1024 --partitions 8 --producers 4 --consumers 4 --compression lz4 --flush-policy periodic --flush-interval-ms 1000 --flush-bytes 67108864"
```

Use `--keep-data` to retain the generated segment directory for inspection. The harness is intentionally not part of `sbt test`, so ordinary CI runs remain deterministic and quick.

The latest measured run, including exact verification of all one million consumed records, is recorded in [`docs/performance/2026-08-05-heavy-load.md`](docs/performance/2026-08-05-heavy-load.md).

## Current limits and roadmap

The current milestone is a durable, language-neutral log with classic consumer groups and a static replicated-cluster vertical slice. It intentionally does not advertise APIs it cannot honor. It is not yet a safe replacement for a Kafka production cluster; the measurable release gates are tracked in [`docs/production-readiness.md`](docs/production-readiness.md). The next compatibility milestones are:

1. Quorum controller election, dynamic membership, replica catch-up/re-admission, persisted high-watermark reconciliation, reassignment, and replicated group/offset state.
2. Idempotent/transactional producers and producer-state recovery.
3. Retention, log compaction, timestamp indexes, and offset-journal compaction/expiry.
4. TLS/SASL, ACLs, quotas, metrics, health checks, and complete administrative operations.
5. The newer consumer group protocol, static-member fencing, rolling-upgrade guarantees, chaos/soak qualification, and broader protocol-version coverage.
6. Zero-copy fetch (`FileChannel.transferTo`) and dedicated selector/worker pools when profiles show they improve the existing end-to-end baseline.

## Configuration

| Option | Default | Meaning |
| --- | --- | --- |
| `--host` | `0.0.0.0` | Listener bind host |
| `--port` | `9092` | Listener port; `0` chooses a free port for tests |
| `--advertised-host` | `localhost` | Host returned in Metadata |
| `--advertised-port` | listener port | Port returned in Metadata |
| `--data-dir` | `data` | Topic and partition segment root |
| `--max-request-bytes` | `104857600` | Hard frame-size bound |
| `--segment-bytes` | `134217728` | Segment rollover target |
| `--flush-policy` | `periodic` | `periodic` batched background forces or strict per-append `sync` |
| `--flush-interval-ms` | `1000` | Maximum dirty age before a periodic force |
| `--flush-bytes` | `67108864` | Dirty-byte threshold that schedules a force |
| `--node-id` | `1` | Broker/controller ID |
| `--cluster-nodes` | empty | Static comma-separated voters as `id@host:port`; empty keeps single-node mode |
| `--controller-id` | `1` | Fixed metadata and classic-group controller in cluster mode |
| `--default-replication-factor` | `1` | Replication factor used for auto-created topics |
| `--min-insync-replicas` | `1` | Minimum ISR required for `acks=all` |
| `--peer-timeout-ms` | `3000` | Internal metadata and replica RPC timeout |
| `--no-auto-create` | off | Disable Metadata/Produce auto-creation |

## License

Apache License 2.0.

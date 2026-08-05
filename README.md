# Cascade

Cascade is a single-node, Kafka-wire-compatible streaming log implemented in Scala 3. It is an executable vertical slice of a Kafka-like broker: current Kafka clients can create topics, discover partitions, produce compressed or uncompressed magic-v2 record batches, list offsets, and consume with explicit partition assignment.

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

Any Kafka client can then use `localhost:9092` as its bootstrap server. Idempotent production and consumer-group subscription are not in the current compatibility slice; configure producers with `enable.idempotence=false` and consumers with explicit partition assignment.

## Wire compatibility

Cascade returns this exact matrix from `ApiVersions`:

| API | Key | Versions | Implemented behavior |
| --- | ---: | ---: | --- |
| Produce | 0 | 3 | `acks` 0/1/all, batch append, broker offset assignment |
| Fetch | 1 | 6 | partition reads, high watermark, log start offset, batch-aligned limits |
| ListOffsets | 2 | 2 | earliest (`-2`) and latest (`-1`) offsets |
| Metadata | 3 | 4 | broker/topic/partition discovery and optional auto-creation |
| ApiVersions | 18 | 0–4 | legacy and flexible encodings, tagged fields |
| CreateTopics | 19 | 2 | single-node topics and validation-only requests |

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
        ├── metadata/topic registry
        └── partition append log
                ├── Kafka magic-v2 batches stored verbatim
                ├── broker-assigned base offsets
                ├── size-based segment rollover
                └── startup index recovery by linear scan
```

The record hot path does not deserialize records or decompress client batches. Cascade changes only the batch base offset—which is outside the record-batch CRC region—and persists the original batch bytes. This keeps Snappy, LZ4, Zstd, and gzip payload compatibility in the client, where compression is already implemented.

Concurrency is deliberately simple: Java 21 virtual threads isolate slow connections, requests remain ordered within each connection, and each partition log serializes offset assignment with its append. File-channel positional I/O prevents shared channel-position races. Fetches are batch aligned, as required by Kafka's record-set semantics.

## Tests

`sbt test` runs all three layers:

- Unit: primitive/flexible wire codecs, bounds failures, record-batch offsets, segment rollover, and recovery.
- Integration: a real TCP socket sends hand-encoded ApiVersions, Metadata, Produce, and Fetch frames over one persistent connection.
- End to end: Apache Kafka client 4.3.1 creates a topic through `Admin`, writes through `KafkaProducer`, and reads through `KafkaConsumer`.

## Current limits and roadmap

The current milestone is a durable, language-neutral, single-node log. It intentionally does not advertise APIs it cannot honor. The next compatibility milestones are:

1. Consumer group coordination and durable committed offsets.
2. Idempotent/transactional producers and producer-state recovery.
3. Multi-node controller metadata, leader epochs, replication, and ISR management.
4. Retention, compaction, timestamp indexes, quotas, metrics, TLS/SASL, and ACLs.
5. Zero-copy fetch (`FileChannel.transferTo`) and dedicated selector/worker pools after a reproducible JMH and end-to-end throughput baseline.

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
| `--node-id` | `1` | Broker/controller ID |
| `--no-auto-create` | off | Disable Metadata/Produce auto-creation |

## License

Apache License 2.0.


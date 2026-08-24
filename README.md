# Cascade

<p align="center">
  <img src="docs/assets/cascade-logo.png" alt="Cascade logo: high-speed cascading data streams" width="960">
</p>

> I'm building a fast Kafka-style streaming log in pure Scala 3.

I built Cascade around the Kafka wire protocol so existing Kafka clients can connect without a custom SDK. If a language has a client that speaks one of the supported protocol versions, it can talk to Cascade.

The broker itself only needs Scala and the JDK. I use Apache Kafka's Java client in the test suite as an independent compatibility check; it isn't a runtime dependency.

So far, I've implemented broker-assigned offsets, magic-v2 record batches, consumer coordination, durable metadata and offset journals, idempotent producer recovery, transactions, `read_committed` isolation, ISR replication, partition-leader promotion, quorum controller election, coordinator failover, online partition reassignment, dynamic broker/voter membership, crash-safe storage lifecycle management, TLS, Kafka SASL/PLAIN, resource ACLs, security auditing, live credential/policy rotation, and broker admission controls.

> [!IMPORTANT]
> Cascade isn't a production Kafka replacement yet. I now protect the client listener with TLS or SASL/TLS, reload salted credentials and deny-by-default ACLs, persist audit decisions, and bound connections, in-flight requests, and principal ingress rates. I have tested those paths with Kafka 4.3.1 Admin, Producer, and group Consumer clients. Secure broker-to-broker transport, additional SASL mechanisms, production observability, physical power/device-loss qualification, and per-topic lifecycle policy are still release blockers.

## Performance I measured

I measured this on my Windows development machine with Java 21, eight partitions, four producers, four consumers, 1 KiB deterministic incompressible payloads, LZ4, `acks=all`, and periodic background flushing:

| Workload | Produce | Consume | Exact verification |
| --- | ---: | ---: | ---: |
| 10,000,000 records, sustained | **182,285 records/s** / **178.0 MiB/s** | **473,058 records/s** | **10,000,000 / 10,000,000** |
| 1,000,000 records, calibration | **614,413 records/s** / **600.0 MiB/s** | **556,232 records/s** / **543.2 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, coordinator-failover regression | **510,714 records/s** / **498.7 MiB/s** | **263,637 records/s** / **257.5 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, fault-qualification regression | **503,499 records/s** / **491.7 MiB/s** | **250,190 records/s** / **244.3 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, storage-lifecycle regression | **423,662 records/s** / **413.7 MiB/s** | **477,368 records/s** / **466.2 MiB/s** | **1,000,000 / 1,000,000** |
| 1,000,000 records, security/isolation regression | **544,611 records/s** / **531.8 MiB/s** | **291,753 records/s** / **284.9 MiB/s** | **1,000,000 / 1,000,000** |

After I fixed the background-flush path, sustained ten-million-record production went from 57,400 to 182,285 records/s: a **3.18x improvement**. The write phase dropped from 174.2 to 54.9 seconds. That run forced 9.58 GiB in 47.6 cumulative seconds, so the drive was the main limit on this machine.

The one-million test is much shorter and benefits a lot from the filesystem cache. I don't present either result as production capacity. The [full heavy-load report](docs/performance/2026-08-05-heavy-load.md) includes the machine, workload, latency, CPU, GC, heap, storage, and test method.

## What I'm building

| Area | What is implemented |
| --- | --- |
| Language-neutral access | Length-prefixed Kafka TCP frames and an explicit `ApiVersions` contract; no custom client library required |
| Pure Scala/JDK runtime | Scala 3 broker implementation with Java 21 virtual threads and positional file I/O |
| Efficient record path | Kafka magic-v2 batches remain compressed and opaque; the broker updates only the base offset outside the batch CRC region |
| Delivery guarantees | Producer IDs, epoch fencing, bounded duplicate detection, sequence recovery, transactions, timeouts, transactional offsets, and `read_committed` |
| Durable state | CRC32C-protected local journals plus one versioned, quorum-committed coordinator image for groups, offsets, producers, and transactions |
| Consumer coordination | Classic join, sync, heartbeat, leave, rebalance, session expiry, and durable committed offsets |
| Dynamic cluster | Durable joint-consensus membership, Kafka Admin add/remove/describe APIs, controller election and fencing, synchronous ISR replication, persisted committed high watermarks, leader promotion, incremental divergent-tail repair, and safe replica re-admission |
| Failure qualification | Deterministic directional partitions and protocol-triggered drops, subprocess force kills, clean/unclean startup detection, torn-tail recovery, and stable/joint quorum safety checks |
| Storage lifecycle | Scheduled time/size retention, conservative keyed compaction, offset expiry, bounded coordinator journals, batch timestamp/transaction indexes, atomic retirement, and disk-reserve admission |
| Security and isolation | TLS 1.2/1.3, Kafka SASL/PLAIN, PBKDF2 credentials, deny-by-default ACLs, JSONL audit events, live credential/ACL rotation, connection/request caps, principal ingress quotas, and overload shedding |
| Measured performance | Repeatable one-million and ten-million tests with exact record counting, latency, CPU, GC, heap, storage, and flush metrics |

## What works now

### Kafka-compatible networking

- Persistent, length-delimited, big-endian Kafka TCP frames.
- Hard request-size bounds and version validation before request handling.
- `SSL`, `SASL_PLAINTEXT`, and `SASL_SSL` listeners with TLS 1.2/1.3 and optional client-certificate verification.
- Kafka-framed `SaslHandshake` v1 and `SaslAuthenticate` v1 with SASL/PLAIN identities scoped to one connection.
- Global/per-IP connection caps, a bounded global in-flight request gate, and per-principal request-byte token buckets.
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
- Scheduled time and per-partition size retention only retire closed, fully committed segments and preserve the active segment.
- Segment retirement uses an atomic `.deleted` rename; startup finishes an interrupted deletion without exposing the retired log again.
- Conservative keyed compaction rewrites closed segments atomically and removes a batch only when every key has a strictly newer offset.
- Compaction keeps compressed, malformed, keyless, control, and transactional batches opaque and untouched.
- Batch timestamp and transaction-range indexes rebuild from immutable headers at startup; `ListOffsets` now returns the first batch whose maximum timestamp reaches the requested time.
- Offset expiry is committed through the same local or quorum coordinator checkpoint as ordinary offset changes.
- Offset, delivery, and cluster-metadata journals compact to their latest checksum-protected image after a configurable byte threshold.
- A configurable free-space reserve rejects an append before its log end changes and returns Kafka error 56 (`KAFKA_STORAGE_ERROR`).

Kafka acknowledgements and local disk forcing are separate settings. In single-node periodic mode, `acks=1` and `acks=all` acknowledge a local append before the next scheduled force. A process, OS, or power failure before that force can lose those records. In cluster mode, `acks=all` requires the configured minimum ISR and waits for every current ISR member to append before the committed high watermark advances.

### Idempotence and transactions

In single-node and clustered modes I support this Kafka delivery-semantics path:

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

In cluster mode I commit producer registration, fencing epochs, active transaction ranges, outcomes, and transactional consumer offsets as one versioned coordinator image. A stale broker can only propose against the coordinator version it installed, so it cannot overwrite newer state. If the quorum rejects a transition, I restore the last acknowledged image and return a retriable coordinator error. My Kafka-client test commits an open transaction and its staged offset after the original coordinator stops, then verifies `read_committed` visibility and successor producer initialization.

### Consumer groups

- Classic Kafka group protocol with member ID allocation.
- Join and sync phases, protocol selection, and leader-provided assignments.
- Heartbeats, session expiration, explicit leave, and rebalance initiation.
- Generation/member validation for ordinary offset commits.
- Append-only CRC32C-protected offset journal with one forced commit per multi-partition request.
- Offset recovery across restart and partial/corrupt-tail truncation.
- Transactional offset staging and commit through `TxnOffsetCommit`.
- Quorum snapshots of group generations, members, assignments, pending identities, and committed offsets.
- Controller-term ownership, stale-image rejection, and Kafka-client continuation after coordinator failover.

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
- One atomic, versioned coordinator image replicated by the metadata quorum and installed on every synchronized broker.
- Optimistic coordinator-version fencing for state proposals forwarded by non-controller partition leaders.
- Controller-only group/transaction expiration so a follower timer cannot overwrite live coordinator state.
- Injectable peer transports with deterministic directional API drops and partitions for repeatable fault qualification.
- Subprocess force-kill recovery tests that distinguish clean and unclean startup without relying on shutdown hooks.
- Exact data, transaction, and committed-offset recovery after a forced JVM kill, including conservative truncation of torn data and coordinator journal tails.
- Majority availability through an active-controller partition, minority coordinator fencing, durable joint-transition resume, joint-controller loss, and rejection of metadata writes unless both joint voter majorities are present.

The configured node list bootstraps the first committed voter set and gives observers discovery endpoints. After that, the committed metadata image is authoritative. Recovery is incremental at Kafka batch boundaries; only its final delta and ISR admission are partition-fenced. Coordinator mutations are acknowledged only after a quorum installs the combined group/offset/producer/transaction image. This is deliberately a single-writer design today; sharding and compacting coordinator state remains a scalability task.

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

### Protect a client listener

I use `SASL_SSL` for password authentication because SASL/PLAIN sends the password inside the TLS tunnel. I keep the key-store password in a separate file instead of a command-line argument.

I generate a salted PBKDF2-SHA-256 credential line like this, then copy the final `alice=pbkdf2-sha256$...` line into `users.conf`:

```powershell
Set-Content -NoNewline alice.password "replace-with-a-long-random-secret"
.\sbt.bat "runMain cascade.security.CredentialTool alice --password-file alice.password"
Remove-Item -LiteralPath alice.password
```

My ACL file uses `effect principal operation resource-type resource-pattern`. A final `*` makes a prefix rule, a rule containing only `*` matches everything, and an explicit deny wins over every matching allow:

```text
allow alice Create Topic orders
allow alice Describe Topic orders
allow alice Write Topic orders
allow alice Read Topic orders
allow alice Read Group order-workers
allow alice Describe Group order-workers
deny alice Write Topic orders-private
```

I start the protected listener with a PKCS12 or JKS key store:

```powershell
.\sbt.bat "run --host 0.0.0.0 --port 9093 --advertised-host broker.example.com --data-dir data --security-protocol SASL_SSL --ssl-keystore broker.p12 --ssl-keystore-password-file broker-store.password --credentials-file users.conf --acl-file acls.conf --audit-log security-audit.jsonl --max-connections 10000 --max-connections-per-ip 1000 --max-inflight-requests 10000 --request-bytes-per-second 104857600 --request-burst-bytes 209715200 --max-throttle-ms 1000"
```

The matching Kafka client properties are:

```properties
bootstrap.servers=broker.example.com:9093
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="alice" password="replace-with-a-long-random-secret";
ssl.truststore.location=cluster-ca.p12
ssl.truststore.password=replace-with-the-truststore-password
ssl.truststore.type=PKCS12
group.protocol=classic
```

Credential and ACL snapshots reload on their configured interval. A malformed replacement never replaces the last valid in-memory snapshot. TLS key material is loaded at startup, so I still use a rolling restart to rotate the listener certificate.

### Bootstrap a three-node cluster for development

Run each command in a separate process and give every broker its own data directory:

```powershell
.\sbt.bat "run --host 127.0.0.1 --port 9092 --advertised-host 127.0.0.1 --advertised-port 9092 --node-id 1 --data-dir data-1 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9093 --advertised-host 127.0.0.1 --advertised-port 9093 --node-id 2 --data-dir data-2 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
.\sbt.bat "run --host 127.0.0.1 --port 9094 --advertised-host 127.0.0.1 --advertised-port 9094 --node-id 3 --data-dir data-3 --cluster-nodes 1@127.0.0.1:9092,2@127.0.0.1:9093,3@127.0.0.1:9094 --controller-id 1 --default-replication-factor 3 --min-insync-replicas 2"
```

Idempotent and transactional Kafka producers can use the cluster path. For meaningful durability, I use at least three voters, replication factor three, `min.insync.replicas=2`, and `acks=all`.

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
| SaslHandshake | 17 | 1 | Negotiate the `PLAIN` mechanism before authentication |
| ApiVersions | 18 | 0-4 | Legacy and flexible encodings with tagged fields |
| CreateTopics | 19 | 2 | Validation and quorum-committed topic metadata |
| InitProducerId | 22 | 1 | Durable producer IDs, epoch allocation, and fencing |
| AddPartitionsToTxn | 24 | 1 | Transaction partition enrollment and timeout start |
| AddOffsetsToTxn | 25 | 1 | Consumer-group enrollment in a transaction |
| EndTxn | 26 | 1 | Durable commit/abort outcome and offset-application checkpoint |
| TxnOffsetCommit | 28 | 2 | Staged offsets made visible only by transaction commit |
| SaslAuthenticate | 36 | 1 | Kafka-framed SASL/PLAIN exchange and session lifetime |
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
        | optional TLS + Kafka length-delimited TCP frames
        v
Virtual-thread connection handler
        |
        | connection cap + SASL identity + request quota/admission
        v
Request routing
        |
        +-- deny-by-default resource ACLs + durable JSONL audit
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

After adding deterministic fault injection and forced-kill recovery qualification, I ran a clean **99/99** test suite and repeated the workload on 2026-08-20. It verified exactly **1,000,000 / 1,000,000** records at **503,499 produced records/s** (**491.7 MiB/s**) and **250,190 consumed records/s** (**244.3 MiB/s**). Produce took 1.986 seconds, consume took 3.997 seconds, p99 acknowledgement latency stayed at or below 500 ms, maximum acknowledgement latency was 445.075 ms, and peak heap was 1,222.1 MiB. I use this short cache-assisted run as a correctness and hot-path regression gate, not as a production capacity claim.

After adding storage lifecycle management, I ran a clean **125/125** test suite and repeated the workload on 2026-08-22. It verified exactly **1,000,000 / 1,000,000** records at **423,662 produced records/s** (**413.7 MiB/s**) and **477,368 consumed records/s** (**466.2 MiB/s**). Produce took 2.360 seconds, consume took 2.095 seconds, p99 acknowledgement latency stayed at or below 1,000 ms, maximum acknowledgement latency was 643.996 ms, and peak heap was 1,747.6 MiB. The lifecycle interval is five minutes by default, so no cleanup ran during this seven-second regression; I use it to detect hot-path and exactness regressions, not to claim lifecycle throughput.

After adding client-listener security and resource isolation, I ran a clean **149/149** suite, added two final rotation/tool checks, and repeated the one-million workload twice on 2026-08-24. Both runs verified exactly **1,000,000 / 1,000,000** records. The repeat produced **544,611 records/s** (**531.8 MiB/s**) and consumed **291,753 records/s** (**284.9 MiB/s**); the first run measured 545,911 and 295,007 records/s. Produce p99 stayed at or below 1,000 ms and the repeat's maximum was 537.726 ms. The plaintext load harness leaves authentication, ACLs, and quotas disabled, so I use this as a default-path regression gate rather than a security-capacity benchmark.

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

The current test suite passes **151/151 tests** in four layers:

- Unit tests for binary codecs, bounds failures, record-batch metadata and sequence wrap, storage pagination, segment rollover, flush policies, corrupt/partial-tail recovery, durable high-watermark recovery, incremental suffix truncation, recovery fingerprints, timestamp and transaction indexes, lifecycle behavior, delivery and group coordination, metadata/controller recovery, TLS contexts, PBKDF2 credential files, atomic credential/ACL reloads, ACL precedence, audit escaping, connection/request admission, and token-bucket quotas.
- TCP integration tests for discovery, Produce/Fetch, acknowledgement behavior, duplicate retry offsets, sequence-gap rejection, idempotent state recovery, flexible voter framing, TLS and SASL negotiation, encrypted authentication and credential rotation, ACL errors and live policy rotation, security auditing, connection caps, and quota shedding.
- Kafka 4.3.1 end-to-end tests for Admin/Producer/Consumer interoperability, a full `SASL_SSL` + ACL Producer/group Consumer flow, classic group rebalances, committed offsets across restart and coordinator loss, RF=3/RF=4 replication, ISR/leader failover, controller election and stale-term fencing, divergent-tail replacement, safe replica re-admission, reassignment, dynamic voters, transactions, and an open transaction committed after coordinator failover.
- Qualification tests that force-kill real broker JVMs, corrupt persisted tails, partition stable and joint quorums, exercise retention through a real Kafka client and broker restart, surface low-disk rejection through the Kafka protocol, and verify exact recovery, minority fencing, transition resumption, and dual-majority write safety.

The load harness separately checks the exact record count at one million and ten million records.

## What I plan to build next

| Priority | Area | Planned work |
| ---: | --- | --- |
| 1 | Operations | Metrics export, health/readiness endpoints, structured logs, admin API coverage, backup/restore, capacity alerts, and operational runbooks |
| 2 | Security hardening | Secure broker-to-broker transport, SCRAM, certificate hot reload, Kafka ACL Admin APIs, egress quotas, and multi-tenant soak tests |
| 3 | Compatibility | New Kafka consumer protocol, static-member fencing, more API versions, malformed-frame/fuzz testing, multiple client languages, and rolling upgrade/downgrade testing |
| 4 | Qualification | Physical power loss, disk loss/full/corruption, arbitrary packet delay/reordering, multi-day soak, and dedicated-host replicated-cluster benchmarks |
| 5 | Coordinator scale | Sharded coordinator state with high-cardinality and failover benchmarks |
| 6 | Storage lifecycle follow-up | Per-topic policies, compressed-batch record compaction, tombstone grace periods, deletion throttles, and replicated retention coordination |
| 7 | Profile-driven optimization | Zero-copy Fetch with `FileChannel.transferTo`, selector/worker pools, multi-device log placement, and further changes justified by profiling |

I track the release gates in [docs/production-readiness.md](docs/production-readiness.md). I won't call Cascade a production Kafka replacement until every blocking gate passes on the deployment topology I document.

## What is still missing

- Dynamic membership is implemented and deterministic stable/joint partition tests pass, but automatic broker registration, rolling version negotiation, real OS power-loss testing, and exhaustive failure schedules during every joint phase are not complete.
- Replica recovery and reassignment transfer bounded record-batch chunks rather than zero-copy segment files; Produce is briefly fenced for the final delta and metadata transition.
- Coordinator failover uses one full quorum image and one elected writer. The local journals are bounded and offsets expire, but I still need sharding and high-cardinality scale tests before treating it like Kafka's partitioned internal topics.
- Lifecycle settings are broker-wide. I still need durable per-topic policies and Kafka configuration APIs.
- Key compaction is deliberately batch-conservative: compressed, keyless, control, transactional, and partially superseded batches stay intact, and tombstone grace-period deletion is not implemented.
- The client listener supports TLS and SASL/PLAIN, but broker-to-broker traffic does not yet authenticate or encrypt itself; I also still need SCRAM/OAuth mechanisms and certificate hot reload.
- ACL files are local operator-managed policy, not the Kafka ACL Admin APIs. Topic data paths return Kafka authorization errors, while some denied control APIs currently close the connection.
- The current quota measures inbound request bytes per principal. I still need response/egress, produce/fetch-specific, and cluster-wide distributed quotas plus multi-tenant soak qualification.
- Structured operational endpoints, metrics export, backup/restore, and deletion I/O throttling are not implemented.
- Only the classic consumer group protocol is supported.
- The automated client matrix currently uses Kafka Java client 4.3.1.
- The performance figures are single-node, shared-JVM development-machine measurements; replicated-cluster capacity has not been benchmarked.
- The forced-kill harness validates process loss and torn tails, but it does not yet simulate lost devices, disk-full errors, controller-host power loss, or filesystem/controller write reordering.

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
| `--cleanup-policy` | `delete` | Broker-wide `delete`, `compact`, or combined `delete,compact` lifecycle policy |
| `--retention-ms` | `604800000` | Age limit for closed committed segments; `-1` disables time retention |
| `--retention-bytes` | `-1` | Per-partition byte budget; `-1` disables size retention |
| `--lifecycle-interval-ms` | `300000` | Interval between lifecycle maintenance passes |
| `--minimum-free-bytes` | `0` | Free-space reserve below which Cascade rejects new appends before writing |
| `--offset-retention-ms` | `604800000` | Age limit for committed consumer offsets; `-1` disables expiry |
| `--journal-compaction-bytes` | `67108864` | Local offset, delivery, and cluster-metadata journal compaction threshold |
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
| `--security-protocol` | `PLAINTEXT` | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` client listener |
| `--ssl-keystore` | Empty | PKCS12 or JKS server key store; required by `SSL` and `SASL_SSL` |
| `--ssl-keystore-password-file` | Empty | UTF-8 file containing the key-store password |
| `--ssl-key-password-file` | Key-store password | Optional separate private-key password file |
| `--ssl-truststore` | JVM default | PKCS12 or JKS client trust store; required when client certificates are requested |
| `--ssl-truststore-password-file` | Empty | UTF-8 file containing the trust-store password |
| `--ssl-client-auth` | `none` | `none`, `requested`, or `required` TLS client-certificate verification |
| `--tls-protocols` | `TLSv1.3,TLSv1.2` | Enabled TLS protocol list |
| `--credentials-file` | Empty | PBKDF2 credential file required by SASL listeners |
| `--credential-reload-ms` | `1000` | Interval for atomic credential snapshot reload |
| `--sasl-session-lifetime-ms` | `0` | Session lifetime reported by `SaslAuthenticate`; zero disables reauthentication expiry |
| `--acl-file` | Empty | Deny-by-default resource ACL file; no file leaves authorization disabled |
| `--acl-reload-ms` | `1000` | Interval for atomic ACL snapshot reload |
| `--super-users` | Empty | Comma-separated principals that bypass ACL evaluation |
| `--audit-log` | Empty | Append-only JSONL destination for authentication and authorization events |
| `--audit-buffered` | Off | Skip per-event `force(false)`; shutdown still forces and closes the audit log |
| `--max-connections` | `10000` | Global active connection cap |
| `--max-connections-per-ip` | `1000` | Active connection cap for one source IP |
| `--max-inflight-requests` | `10000` | Global request permits before overload shedding closes a connection |
| `--request-bytes-per-second` | `0` | Per-principal ingress quota; zero disables quota work |
| `--request-burst-bytes` | Quota rate | Per-principal token-bucket burst size |
| `--max-throttle-ms` | `1000` | Maximum quota delay; larger required delays shed the request connection |
| `--no-auto-create` | Off | Disable Metadata/Produce auto-creation |

## More documentation

- [Production-readiness gates](docs/production-readiness.md)
- [Heavy-load report](docs/performance/2026-08-05-heavy-load.md)
- [Contributing](CONTRIBUTING.md)
- [Apache-2.0 license](LICENSE)

## License

Apache License 2.0.

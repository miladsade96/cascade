# Bounded coordinator commit batching

I batch ordinary Kafka `OffsetCommit` requests on each clustered broker to amortize full-state serialization and quorum publication. This is a capacity increment, not independent shard consensus: the metadata quorum and combined group/delivery service lock still serialize publication. Single-node local-journal commits and transactional offset checkpoints keep their existing paths.

## Publication and failure contract

- I admit immutable commands into one FIFO worker per clustered broker. Both queued and in-flight commands count against request and estimated retained-byte limits. A batch has separate count and byte bounds.
- The worker rechecks broker ownership/readiness, queue age, group generation, static-member identity, and group-key consistency while holding the coordinator service lock, immediately before staging. Validation failures remain per request; they do not invalidate unrelated valid commands.
- Valid commands are applied in arrival order and share one atomic checkpoint. Repeated keys are last-write-wins, including deliberate offset rewinds. Separate connections have no ordering guarantee before queue admission. Transactional offsets remain coupled to their transaction outcome, never this queue.
- Success waits for the existing durable publication result. Failed quorum publication restores authoritative state for every staged command. A lost response can still leave an ambiguous outcome; clients must retry according to Kafka semantics.
- Offset reads take the service lock and cannot observe staged values before publication or after rollback. Generation validation and mutation now use the same critical section.
- OffsetFetch captures readiness and all requested offsets in one locked view. It reuses that decision for every response error field; a readiness transition cannot turn an unavailable lookup into a successful missing-offset response.
- Queue timeout or interruption can cancel a queued/claimed command before staging. The worker rechecks cancellation after acquiring the service lock. Once staging starts, the caller waits for the actual outcome even if interrupted, then restores its interrupt status. This preserves the broker's request/snapshot barrier.
- Shutdown refuses new work, fails pending work, and drains active publication before closing coordinator storage. It does not interrupt a quorum force. Shutdown duration therefore still depends on the underlying publication path; there is no hard wall-clock shutdown guarantee.

The request identity and error contract follows the supported versions in the [Apache Kafka protocol](https://kafka.apache.org/40/design/protocol/). Batching introduces no wire version, metadata format, feature level, or dependency change. The supported OffsetCommit range remains v5-v7; this milestone does not add newer consumer-protocol offset validation or administrative group APIs.

## Bounds and tuning

| CLI option | Default | Meaning |
| --- | ---: | --- |
| `--offset-batch-max-requests` | 64 | Maximum commands in one checkpoint batch; 1 is the single-request control |
| `--offset-batch-max-bytes` | 1048576 | Maximum estimated retained command bytes in a batch |
| `--offset-batch-pending-requests` | 1024 | Maximum queued plus in-flight commands |
| `--offset-batch-pending-bytes` | 16777216 | Maximum estimated queued plus in-flight command bytes |
| `--offset-batch-linger-ms` | 2 | Maximum intentional accumulation delay; full count dispatches immediately |
| `--offset-batch-queue-timeout-ms` | 5000 | Maximum time before staging admission, not a quorum-publication deadline |

Each bound is validated before threads start. Linger is limited to 100 ms, queue timeout to 60 seconds, batch count to 1024, and pending count to 65536. Byte limits are 1 KiB..16 MiB per batch and at most 256 MiB pending. Pending limits must cover one full batch. CLI options are applied in order: raise pending limits before batch limits when necessary.

Accounting uses conservative UTF-16 string and object allowances with 64-bit arithmetic. It is **not** a full JVM heap cap: wire frames, decoder objects, snapshots, and metadata replication remain subject to their own existing limits. A single command larger than the batch byte limit returns `INVALID_REQUEST`; count/aggregate-byte pressure returns retriable `REQUEST_TIMED_OUT`; closure returns `COORDINATOR_NOT_AVAILABLE`. A rejected command is never staged. Existing connection/request admission and principal quotas still apply before batching; the queue adds no tenant-specific fairness guarantee.

I inspect these read-only values through Kafka `DescribeConfigs` under the `cascade.offset.batch.*` names. Changes require a broker restart. I use `max-requests=1` with `linger-ms=0` for a matched control, not to bypass the queue's bounds and lifecycle safeguards. I set client API/request timeouts to cover realistic queue plus publication and retry latency; a timeout is not proof that an offset was not committed.

## Metrics and acceptance

The eleven new Prometheus series use only `node_id`. `cascade_offset_batch_pending_requests`/`pending_bytes` include in-flight retention; `peak_requests`/`peak_bytes` are high-water marks since startup. `accepted_total`, `rejected_total`, `completed_total`, and `failed_total` distinguish pre-admission rejection from admitted outcomes. `cascade_offset_batches_total` and `cascade_offset_batch_requests_total` measure dispatched batches and requests, including publication failures. `cascade_offset_batch_queue_seconds_total` sums wait through staging admission, excluding commands cancelled before admission.

I check retained-work peaks against the configured queue caps, not client-worker concurrency: cancellation or response completion can precede final worker cleanup of an in-flight command.

I compare request/batch deltas over the same interval to measure coalescing, and correlate failed requests with checkpoint failures and client retry latency. I alert on sustained rejected/failed rates or growing pending work before raising limits. A low queue rejection count alone is not a capacity pass.

Unit tests cover bounds, FIFO rewinds, isolated invalid commands, rollback, read isolation, ownership/static-member revalidation, byte splitting, timeout cancellation, interruption, worker exceptions, shutdown drain, and metrics. A real three-broker wire test rejects a batched write under quorum loss, retries, and verifies exact restart recovery. Persistent Kafka-client end-to-end runs compare batching with a single-request control through controller loss and full restart. CI additionally runs 1,000-group persistent controls at 8/32 workers and an 8-worker churn campaign, archiving exact offsets, latency, admission, and coalescing evidence without asserting hardware-independent throughput.

Independent per-shard consensus/publication, finer-grained service locks, less whole-state CPU work, large membership/transaction churn, and dedicated-host RF=3 qualification remain release gates.

For whole-software compatibility on Windows, I run `scripts/qualify-staged-clients.ps1 -Java <Java-21-executable>` after `Test/compile stage` and `npm ci` in `compatibility/node`. It uses Docker, host Node, a dependency-complete staged runtime, port 19092, and five exact client checks plus Java restart recovery. It creates no Cascade image and publishes nothing. Its uniquely named broker container is removed afterward; synthetic data remains under `artifacts/client-data-*` for inspection. This is single-node compatibility coverage, not multi-language clustered batching or a container release. I do not rebuild the staged jars while that container is running.

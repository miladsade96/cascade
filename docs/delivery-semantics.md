# Delivery semantics

I implement Kafka-compatible idempotent production, producer fencing, transactions, transactional offset commits, `read_committed` Fetch, and delivery-state administration for the protocol versions listed in the README. I keep the guarantee tied to acknowledged durable state; a client timeout can still make a write outcome ambiguous and must be handled with Kafka's producer retry rules.

## Idempotent production

I allocate producer IDs durably, advance epochs when ownership changes, reject fenced epochs, and validate each partition sequence. Duplicate retries inside the retained sequence window return the original result instead of appending a second batch. Producer and sequence state recover from the delivery journal and replicated coordinator image.

`InitProducerId` v3-v5 accepts Kafka's expected producer ID and epoch. A retry of a successful initialization returns the already advanced epoch instead of advancing it again. A stale or unrelated identity is fenced. I advertise v0-v5; v6 is not advertised because Kafka marks its two-phase-commit extension unstable.

The broker preserves Kafka magic-v2 record batches. It does not decode and rebuild ordinary Produce traffic, so producer ID, epoch, sequence, compression, CRC32C, and transactional/control markers remain part of the stored batch.

## Transactions

I track transaction registrations, producer epochs, timeout, participating partitions, appended offset ranges, consumer groups, and pending group offsets. Commit or abort publishes one coordinator checkpoint. A commit with pending offsets uses the coordinator's explicit combined operation, so its transaction and group-offset shards enter the same certified quorum transaction even though group and delivery have independent mutation monitors. A commit becomes visible only when its outcome and transactional offsets are acknowledged together. A failure restores the authoritative coordinator image; an old producer epoch cannot finish a newer owner's transaction.

The wire surface includes `AddPartitionsToTxn` v0-v5, `AddOffsetsToTxn` v0-v4, `EndTxn` v0-v4, and `TxnOffsetCommit` v0-v4. Version 3 and newer use flexible framing. Batched partition admission and `verifyOnly` checks are implemented for versions 4 and 5. A verify-only request never enrolls a partition: it returns success only for an already enrolled partition and `TRANSACTION_ABORTABLE` for an unenrolled partition in an active transaction. Transactional offset staging rejects stale consumer generations, member IDs, and modern consumer epochs before changing delivery state.

I deliberately do not advertise `EndTxn` or `TxnOffsetCommit` v5. Those versions are part of Kafka transaction protocol V2 and require semantics beyond a framing change. The version cap keeps negotiation honest until that protocol is implemented and qualified.

Transaction expiry runs on the owning coordinator's scheduled mutation path. It does not run lazily inside Fetch. If expiry is delayed, `read_committed` can conservatively withhold records longer, but it cannot expose an active transaction as committed.

## Read-committed visibility

For each Kafka Fetch response I capture one immutable acknowledged `DeliveryReadView`. The view contains:

- the earliest active transactional offset for every topic-partition;
- completed outcomes indexed by producer ID and epoch, newest first; and
- the acknowledged `offsetsApplied` state used to couple a committed outcome to its group offsets.

The response calculates every last stable offset and transactional-batch decision from that same view. Non-transactional batches remain visible. A transactional batch is visible only when a covering outcome for its exact producer ID/epoch is committed and its offsets were applied. An aborted, active, unknown, wrong-epoch, or partially applied outcome is withheld.

The view changes only after a checkpoint succeeds or an authoritative image installs. A transaction completing in parallel cannot produce a response with the new last stable offset and old batch decisions, or the reverse. The detailed concurrency contract is in [acknowledged coordinator read isolation](coordinator-read-isolation.md).

## Delivery administration

Kafka Admin can inspect acknowledged delivery state through:

- `DescribeProducers` v0, including producer ID, epoch, last sequence, last timestamp, and the current transaction's first retained offset;
- `DescribeTransactions` v0, including coordinator state, timeout, start time, producer identity, and enrolled partitions; and
- `ListTransactions` v0-v2 with state, producer ID, duration, and regular-expression filters.

The handlers use immutable acknowledged coordinator views, enforce Kafka topic and transactional-ID ACLs, route to the owning coordinator, and return deterministic ordering. They never expose state that was staged locally but failed quorum publication.

## Tests I require

I cover sequence acceptance, duplicates, gaps, fencing, restart recovery, transaction timeout, commit/abort, transactional offsets, corrupt/torn journal recovery, cluster failover, and exact Kafka-client `read_committed` consumption. Read-view tests additionally cover earliest active ranges, newest covering outcome, unapplied offsets, producer epochs, immutable captured views, and accepted/rejected checkpoints blocked after staging. Protocol tests cover legacy and flexible layouts, batched partition admission, verify-only checks, expected-epoch retries, transaction filtering, producer-state recovery, and stale member fencing.

The release gate also runs the full Kafka 4.3.1 end-to-end suite and the three-broker coordinator fault tests. The 2026-09-14 run passed 564/564 tests in 179 seconds. Its transaction churn runner committed and aborted data through 256 transactional IDs at concurrency 16, verified 256 committed and 512 uncommitted records exactly, restarted the broker, and verified the same 256 committed values again with zero mismatches. I keep the exact commands and results in the [delivery qualification report](performance/2026-09-14-delivery-semantics.md). A passing unit suite alone is not delivery qualification.

## Remaining production gates

At metadata format 12 the transaction coordinator uses forced per-shard quorum journals instead of the metadata-controller log. Majority votes produce a durable certificate; a successor queries voter state and must finish a certified transaction rather than abort it. Completed journals compact to atomic forced checkpoints while unresolved records survive replacement. Group and delivery mutations use separate monitors, although delivery mutations still serialize within their own image. I still need high-cardinality concurrent transaction churn on dedicated RF=3 hosts, broader producer/client version matrices, Kafka transaction protocol V2, multi-day authenticated soak evidence, arbitrary packet impairment, and real power/device-loss results before calling Cascade a production Kafka replacement.

# Delivery semantics

I implement Kafka-compatible idempotent production, producer fencing, transactions, transactional offset commits, and `read_committed` Fetch for the protocol versions listed in the README. I keep the guarantee tied to acknowledged durable state; a client timeout can still make a write outcome ambiguous and must be handled with Kafka's producer retry rules.

## Idempotent production

I allocate producer IDs durably, advance epochs when ownership changes, reject fenced epochs, and validate each partition sequence. Duplicate retries inside the retained sequence window return the original result instead of appending a second batch. Producer and sequence state recover from the delivery journal and replicated coordinator image.

The broker preserves Kafka magic-v2 record batches. It does not decode and rebuild ordinary Produce traffic, so producer ID, epoch, sequence, compression, CRC32C, and transactional/control markers remain part of the stored batch.

## Transactions

I track transaction registrations, producer epochs, timeout, participating partitions, appended offset ranges, consumer groups, and pending group offsets. Commit or abort publishes one coordinator checkpoint. A commit with pending offsets uses the coordinator's explicit combined operation, so its transaction and group-offset shards enter the same certified quorum transaction even though group and delivery have independent mutation monitors. A commit becomes visible only when its outcome and transactional offsets are acknowledged together. A failure restores the authoritative coordinator image; an old producer epoch cannot finish a newer owner's transaction.

Transaction expiry runs on the owning coordinator's scheduled mutation path. It does not run lazily inside Fetch. If expiry is delayed, `read_committed` can conservatively withhold records longer, but it cannot expose an active transaction as committed.

## Read-committed visibility

For each Kafka Fetch response I capture one immutable acknowledged `DeliveryReadView`. The view contains:

- the earliest active transactional offset for every topic-partition;
- completed outcomes indexed by producer ID and epoch, newest first; and
- the acknowledged `offsetsApplied` state used to couple a committed outcome to its group offsets.

The response calculates every last stable offset and transactional-batch decision from that same view. Non-transactional batches remain visible. A transactional batch is visible only when a covering outcome for its exact producer ID/epoch is committed and its offsets were applied. An aborted, active, unknown, wrong-epoch, or partially applied outcome is withheld.

The view changes only after a checkpoint succeeds or an authoritative image installs. A transaction completing in parallel cannot produce a response with the new last stable offset and old batch decisions, or the reverse. The detailed concurrency contract is in [acknowledged coordinator read isolation](coordinator-read-isolation.md).

## Tests I require

I cover sequence acceptance, duplicates, gaps, fencing, restart recovery, transaction timeout, commit/abort, transactional offsets, corrupt/torn journal recovery, cluster failover, and exact Kafka-client `read_committed` consumption. Read-view tests additionally cover earliest active ranges, newest covering outcome, unapplied offsets, producer epochs, immutable captured views, and accepted/rejected checkpoints blocked after staging.

The release gate also runs the full Kafka 4.3.1 end-to-end suite and the three-broker coordinator fault tests. The 1.4.0 release run passed 539/539 tests in 153 seconds; its regression closes a coordinator during an open Kafka transaction and verifies both committed data and the exact staged group offset afterward. A passing unit suite alone is not delivery qualification.

## Remaining production gates

At metadata format 12 the transaction coordinator uses forced per-shard quorum journals instead of the metadata-controller log. Majority votes produce a durable certificate; a successor queries voter state and must finish a certified transaction rather than abort it. Completed journals compact to atomic forced checkpoints while unresolved records survive replacement. Group and delivery mutations use separate monitors, although delivery mutations still serialize within their own image. I still need high-cardinality concurrent transaction churn on dedicated RF=3 hosts, broader producer/client version matrices, multi-day authenticated soak evidence, arbitrary packet impairment, and real power/device-loss results before calling Cascade a production Kafka replacement.

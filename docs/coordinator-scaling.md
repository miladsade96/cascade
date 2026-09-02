# Coordinator scaling

I distribute coordinator requests using rendezvous ownership. This milestone separates conflict detection for unrelated state: a request submits only changed virtual shards, and the controller validates every touched shard before publishing one atomic quorum image.

## Safety contract

- I keep a fixed, versioned shard layout. Group membership and that group's offsets share a shard; transaction registrations, active ranges, and outcomes share a transaction shard. Producer-ID allocation has its own serialized shard.
- A multi-shard transaction validates every expected version before applying any change. A stale shard, old controller term, or failed quorum rejects the entire change.
- Feature negotiation gates the new commit RPC and metadata format. Mixed-version clusters continue using the existing whole-image path until every voter supports the new feature.
- Installing a remote image never moves the local state backward. Failed commits restore authoritative state, and changes based on an uninstalled image cannot overwrite acknowledged state.

## Acceptance gates

I require deterministic layout/distribution tests; wire and metadata round trips; independent-shard progress; same-shard conflict rejection; atomic transaction/offset conflicts; producer-ID allocation conflicts; stale-term rejection; concurrent Kafka offset commits; owner/controller failover; exact offset recovery; and the complete regression suite. The scale runner records workload, latency, throughput, revision, and exact verification instead of treating one development-machine result as production capacity.

## Remaining limits

This is shard-scoped conflict isolation and transfer, not independent consensus groups. The controller still serializes metadata publication and each broker retains the complete coordinator state. The subsequent [incremental persistence milestone](incremental-coordinator.md) adds delta journal records and peer replication; full snapshots remain for structural changes, checkpoints, and recovery. Local group/delivery services still share their atomicity lock. I do not claim linear horizontal throughput scaling or dedicated-host production capacity.

## Implemented layout and rollout

Layout 1 has group shards `0..63`, transaction shards `64..127`, and producer-ID allocator shard `128`. I select a bucket from SHA-256 over the UTF-8 key. Registrations with a transactional ID share that transaction's bucket; anonymous registrations use a producer-ID-derived key. A hash collision intentionally serializes the affected keys. Rendezvous request ownership remains independent of the virtual conflict layout.

`coordinator-deltas` level 1 activates only after all voters advertise it and a metadata mutation commits the feature set. The committed metadata then requires format 9. Before activation, I retain the legacy complete-image commit RPC; afterward, I reject that RPC so it cannot erase shard versions. Existing images seed every shard from their acknowledged global version without changing records. I do not change the bucket count in place.

The controller checks the term and every touched shard version before merging an update. Group membership/offset payloads and transaction/producer payloads are validated against their declared shard. The allocator is monotonic, producer IDs must remain unique and within its allocated range, and all touched versions advance in the same quorum image. Empty shard replacement implements deletion. The local checkpoint base is the installed image, not a newer metadata image that the service has not loaded yet.

Image installation uses one virtual-thread worker with at most one pending image. Metadata publication never waits for the service lock. Newer images supersede pending older ones, installation stays monotonic, and readiness compares the relevant shard versions. This removes cross-key fencing caused solely by an unrelated image update; same-shard conflicts and transient fencing can still require client retries.

## Reproduce the qualification

I use Java 21 and run:

```text
./sbt "Test / runMain cascade.qualification.CoordinatorScaleQualification --groups 1000 --concurrency 8 --rounds 2 --report artifacts/coordinator-scale.json"
```

The runner starts three in-process brokers in separate temporary data directories, uses Kafka 4.3.1 classic consumers with manual assignment and synchronous offset commits, and distributes work across bounded client workers. It writes twice to each group, verifies every group's latest offset, stops the controller, verifies the majority's state, writes once more, verifies again, then restarts all brokers from disk and verifies all offsets again. It checks that every broker handled coordinator writes. The full unit/integration/end-to-end suite includes a smaller 60-group, six-worker version.

The JSON identifies the source revision (including a dirty-tree marker), release, Java version, logical CPU count, workload, exact verification, owner IDs, failover/restart status, checkpoint counters, proposed bytes, and commit latency percentiles. `write_seconds` includes client creation/closure in the write phases; it excludes verification and restart time. Latencies cover `commitSync`, including retries. Delta bytes include failed attempts; equivalent full-image bytes are not the total network or disk traffic. A failed campaign writes `status: failed` and exits unsuccessfully. CI archives the report and checks exact counts, recovery, owner coverage, and delta-payload reduction.

This is an offset-cardinality and client-creation campaign, not a high-cardinality membership-rebalance or transaction benchmark. Those, independent shard persistence, finer-grained service locking, multi-day load, and dedicated-host qualification remain separate gates.

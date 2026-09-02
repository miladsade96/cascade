# Incremental coordinator persistence and replication

I am removing complete coordinator images from the steady-state journal and peer commit paths. This is the next coordinator-capacity step, not independent per-shard consensus: publication, in-memory images, and service locks remain shared.

## Safety and acceptance contract

- A delta applies only to the exact committed metadata version, controller term, coordinator version, and touched shard versions from which it was built.
- A delta changes coordinator state only. Membership, topics, feature activation, controller transitions, and recovery use complete snapshots.
- The existing stable/joint quorum rules apply to both encodings. An out-of-date follower rejects a delta and receives a full snapshot.
- One forced CRC32C journal frame covers every shard in an atomic transaction. Replay must reject a checksummed delta with a missing/wrong base; it may discard an incomplete or checksum-corrupt tail under the existing recovery contract.
- Periodic atomic full checkpoints bound replay and retain the existing backup/restore contract.
- A new committed feature and storage-format floor gate activation. Mixed-version clusters retain their previous encoding until every voter supports the new format.
- I require codec/validation tests, torn-tail and checkpoint recovery, wire-level fault tests, Kafka offset/transaction recovery, the complete suite, and measured journal/replication bytes before claiming the optimization works.

Physical power loss, independent shard consensus, fine-grained service locks, multi-day load, and dedicated-host capacity remain separate release gates.

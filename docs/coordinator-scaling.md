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

This is shard-scoped conflict isolation and transfer, not independent consensus groups. The controller still serializes metadata publication, the journal and replication path still carry a complete committed image, and each broker still retains the coordinator state. Local group/delivery services still share their atomicity lock. I do not claim linear horizontal throughput scaling or dedicated-host production capacity from this milestone.

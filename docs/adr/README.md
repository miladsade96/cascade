# Architecture decision records

I use ADRs to preserve why Cascade works the way it does, including options I rejected and costs I accepted. An ADR describes the decision implemented in the repository; it is not a substitute for qualification evidence.

| ADR | Decision |
| ---: | --- |
| [0001](0001-kafka-wire-protocol.md) | Use an explicit tested Kafka wire-protocol subset |
| [0002](0002-segmented-log-storage.md) | Store opaque Kafka batches in segmented append-only logs |
| [0003](0003-replication-model.md) | Use synchronous ISR replication and committed high watermarks |
| [0004](0004-controller-election.md) | Elect and fence controllers through a durable majority |
| [0005](0005-idempotent-producers.md) | Recover bounded producer sequence state durably |
| [0006](0006-consumer-group-coordination.md) | Shard coordination and serve acknowledged immutable views |
| [0007](0007-transactions.md) | Certify transaction outcomes across coordinator shards |
| [0008](0008-virtual-thread-networking.md) | Pair virtual-thread simplicity with strict admission limits |

When a decision changes materially, I add a superseding ADR instead of rewriting the historical reason.


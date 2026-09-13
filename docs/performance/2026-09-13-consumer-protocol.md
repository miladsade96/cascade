# Consumer protocol qualification, 2026-09-13

I qualified the Kafka 4.3.1 consumer-protocol milestone on Windows with Eclipse Adoptium Java 21.0.11 and the repository's Kafka client 4.3.1 test dependency. This is compatibility and correctness evidence from my development machine, not a production capacity result.

## Surface covered

The broker advertises `ConsumerGroupHeartbeat` v0-v1, `ConsumerGroupDescribe` v0-v1, `OffsetCommit` v5-v10, `OffsetFetch` v4-v10, `OffsetDelete` v0, `ListGroups` v0-v5, and `DeleteGroups` v0-v2. Heartbeat v1 accepts consumer-generated member IDs and regex subscriptions. State format 3 persists member identity, regex, group and assignment epochs, and current/target assignments while retaining a backward decoder for format 2.

## Defects found

The Kafka Admin client rejected the first `ConsumerGroupDescribe` response because assignment entries omitted the required topic name between the topic ID and partitions. I corrected the schema and kept the real Admin assertion as a regression.

The first clean full run passed 551 tests and failed the acknowledged-read test because v9/v10 membership validation acquired the mutable group lock during a paused checkpoint. I moved validation to the immutable acknowledged image and made the test's background-client cleanup thread safe. The focused isolation rerun passed in 1.188 seconds.

## Final result

The final complete run passed **552/552** tests with no failures or skips in **182 seconds**. Real Kafka 4.3.1 clients passed explicit and regex modern subscriptions, offset commits and reads using negotiated topic-ID versions, detailed group description, and selected offset deletion. Byte-level tests cover flexible layouts, multi-group fetch, legacy compatibility, active-subscription deletion protection, cooperative reconciliation, fencing, persistence, and format-2 upgrade defaults.

## Remaining boundary

I have not used this run to claim a consumer-group capacity limit. A multi-day authenticated high-cardinality churn campaign and a dedicated-host RF=3 benchmark remain release gates, along with the broader physical power/device-loss and network-impairment work in the production-readiness checklist.

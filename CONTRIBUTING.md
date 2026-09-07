# Contributing

Thanks for helping me improve Cascade.

Please run `sbt test` before opening a pull request. If your change touches the Kafka protocol, I also need:

1. The matching change in `Compatibility.supported`.
2. A codec test for each new version and flexible tagged-field boundary.
3. A raw-socket integration test that checks framing and correlation IDs.
4. An end-to-end test with an independent Kafka client, when that client exposes the API.

I don't want Cascade to advertise compatibility that it can't prove. Before adding a protocol version, make sure every request and response field matches the Apache Kafka grammar. Please keep requests ordered inside each TCP connection and keep offset assignment serialized inside each partition.

If your change touches security or resource isolation, I also need a focused unit test, a broker integration test for the failure boundary, and a real Kafka-client test when the client exposes that path. Please never put a clear password, key-store secret, private key, or generated test key store in the repository. I keep denial behavior fail-closed and preserve the last valid credential or ACL snapshot when a live reload is malformed.

If your change touches metrics, health/readiness, structured events, or capacity alerts, I need a deterministic encoder/evaluator test and an operations-listener integration test. Please keep metric labels bounded; I don't accept topic, client, request, or principal identifiers as Prometheus labels without a measured cardinality budget.

If your change touches coordinator state, I need the acknowledged-read boundary tested separately from staged mutation. Block the checkpoint deterministically after staging and prove readers see the old complete view, then cover both successful publication and rollback. For Fetch paths, capture one immutable view for the complete response; don't mix last-stable-offset and transaction decisions from different generations. Any clustered concurrency change also needs a real three-broker test with an explicit RPC fault, plus failover and restart verification. Please keep sleeps out of correctness triggers and put a timeout on every test barrier.

When you publish coordinator performance evidence, record the exact revision, release, Java version, processor count, workload, client lifecycle, queue settings, admission results, exact recovered values, and latency percentiles. I only compare throughput when the hardware and settings are matched. A lower number of quorum rounds, a clean queue, or a development-host pass is not by itself a production-capacity result.

If your change touches backup or restore, I need tests for traversal, symlinks, unexpected files, corruption, partial publication, existing targets, and exact Kafka-client recovery. I keep the source offline, publish only through a sibling atomic rename, and never weaken manifest verification to make a damaged backup pass.

If you're unsure about the behavior of an API, open an issue before building a large change. I'm happy to discuss the design first.

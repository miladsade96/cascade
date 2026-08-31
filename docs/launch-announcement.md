# Cascade launch announcement

## Main post

Today I'm open sourcing **Cascade**.

I started Cascade because I wanted to understand what it takes to build a Kafka-style streaming system from scratch in pure Scala 3. It speaks the Kafka wire protocol, so existing Kafka clients can connect directly. You don't need a Cascade-specific SDK.

So far, I've built durable segmented storage, Kafka magic-v2 record batches, broker-assigned offsets, idempotent producers, transactions, `read_committed` isolation, classic consumer groups, ISR replication, leader promotion, online partition reassignment, dynamic quorum membership with broker fencing, storage lifecycle management, Kafka-compatible PLAIN, SCRAM-SHA-256/512, and signed OAuth/OIDC OAUTHBEARER authentication, hostname-verified mutual TLS and certificate-bound identities between brokers, resource isolation, Prometheus metrics, health/readiness, structured events, capacity alerts, Kafka Admin configuration reads, checksummed offline backup/restore, and a 38.7 MB non-root distroless container with single/three-broker Compose deployments.

The clean test suite passes **247/247 unit, integration, end-to-end, and fault-qualification tests**. I test with the real Kafka 4.3.1 Java client and cover restart recovery, persisted high watermarks, incremental replica repair, transaction commit and abort, transactional consumer offsets, non-transactional idempotence, group rebalancing, SCRAM-SHA-256/512, signed OAUTHBEARER over TLS with ACLs, live verifier/JWKS rotation, wrong-token denial, encrypted peer replication and quorum failover, certificate impersonation/trust/hostname rejection, online reassignment, dynamic voter changes, low-disk rejection, operations endpoints, Admin `DescribeConfigs`, exact Kafka-visible recovery from a restored backup, and exact container-volume recovery after restart.

I also reran my documented 10-million-record load test. Cascade produced **368,387 records/s (359.8 MiB/s)**, consumed **351,784 records/s (343.5 MiB/s)**, and verified all **10,000,000 records without gaps** on my development machine. I published the setup and full result in the repository. This is a local single-node plaintext benchmark, not a production, OAuth, or TLS capacity claim.

I want to be honest about where the project is today: Cascade isn't a production Kafka replacement yet. It still needs TLS key-store hot reload, Kafka ACL Admin APIs, broader token/key formats and claim-to-role policy, the new consumer protocol, per-topic lifecycle policy, rolling-version qualification, multiple client-language matrices, physical power/device-loss tests, multi-day soak tests, and coordinator scale work. I'm publishing it now because the foundation works, and I want to build the remaining gates in the open.

If you work with Scala, storage engines, distributed systems, Kafka internals, or performance engineering, I'd really like your feedback. Contributions are welcome too.

Project: https://github.com/miladsade96/cascade

#Scala #OpenSource #DistributedSystems #Kafka #Streaming #DataEngineering #JVM

## Short post

I just open sourced **Cascade**, a Kafka-style streaming log I've built from scratch in pure Scala 3.

It speaks the Kafka wire protocol, so existing Kafka clients can use it without a custom SDK.

- Idempotent and transactional delivery with `read_committed`
- Consumer groups, replication, reassignment, controller failover, and dynamic voters
- TLS, PLAIN, SCRAM-SHA-256/512, signed OAuth/OIDC tokens, broker mTLS identities, ACLs, quotas, retention/compaction, Prometheus metrics, and health/readiness
- Checksummed offline backup/restore with exact Kafka-client recovery
- 247/247 automated tests
- 38.7 MB non-root distroless image with single/three-broker Compose examples
- 10 million records verified without gaps
- 368K produced and 351K consumed records/s in my documented development-machine test

It isn't a production Kafka replacement yet. I still have a lot to build, and contributions are welcome.

Project: https://github.com/miladsade96/cascade

#Scala #OpenSource #Kafka #DistributedSystems #DataEngineering

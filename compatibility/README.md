# Kafka client compatibility

I keep this matrix outside the Scala test process so it checks Cascade from the same boundary my users cross. Every client creates or discovers a unique topic, produces 25 ordered records, consumes exactly those 25 records, and fails on any mismatch. The group clients also exercise classic-group coordination and committed offsets.

| Language | Client | Version | Coverage |
| --- | --- | ---: | --- |
| Java | Apache Kafka client | 4.3.1 | Admin, idempotent and transactional producers, explicit and subscribed consumers, failover, security, and recovery |
| JavaScript | KafkaJS | 2.2.4 | Admin, idempotent producer, classic-group consumer, offset commit, and offset fetch |
| Python | confluent-kafka | 2.15.0 | Admin, idempotent producer, and assigned consumer |
| Go | franz-go | 1.21.0 | Admin, idempotent producer, classic-group consumer, and offset commit |
| .NET | Confluent.Kafka | 2.15.0 | Admin, idempotent producer, classic-group consumer, and offset commit |

On Linux or in CI I run the complete external matrix with:

```bash
chmod +x compatibility/run.sh
compatibility/run.sh
```

The runner expects Java 21, Node.js, Python, Go, .NET 8, and `curl`. It builds the runtime tree, starts a private broker on ports 19092 and 19404, installs pinned client dependencies, runs every smoke test, rejects broker-side protocol errors, and shuts the broker down. The normal Scala suite continues to cover the full Java-client and internal correctness surface.

I treat this as a tested client matrix, not a claim that Cascade implements every Kafka API or every client feature. The exact broker API/version contract remains documented in the root README.

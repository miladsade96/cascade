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

The KafkaJS smoke test defaults to a single replica. For the three-broker example with `min.insync.replicas=2`, I set `CASCADE_REPLICATION_FACTOR=3`. A replication-factor-1 topic correctly rejects `acks=all` writes under that cluster policy; I do not lower the broker's durability requirement to make the smoke test pass.

On Windows I can qualify the actual release image, including its declared non-root user, entry point, JVM settings, readiness check, and persistent-data restart:

```powershell
./scripts/qualify-staged-clients.ps1 -Java <Java-21-executable> -BrokerImage miladsade96/cascade:1.3.0 -ExpectedVersion 1.3.0 -GoProxy direct
```

I run `Test/compile` and install the pinned KafkaJS dependencies first. Without `-BrokerImage`, the same script retains its staged-JDK mode and also requires `stage`. Image mode inspects the tag once and runs that immutable image ID, so a tag change cannot silently replace the tested artifact. Synthetic data stays in ignored artifacts; the script removes only its uniquely named broker container. These are single-node checks, not multi-language cluster or physical-loss qualification.

If dependency downloads fail in the broker's shared network namespace, I can first compile `compatibility/go` using `golang:1.25` on Docker's default network, keeping `go.sum` and TLS verification enabled. I then pass that exact Linux binary with `-GoExecutable <absolute-path>`. The client still runs in the broker's network namespace; only dependency acquisition is moved out of it. I record the binary hash and build command with the release evidence rather than treating a skipped Go test as a pass.

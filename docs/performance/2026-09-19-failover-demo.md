# Three-broker leader-failover demo — 2026-09-19

I recorded this run to make the leader-loss path visible and reproducible. It is release-candidate evidence, not a multi-machine availability claim.

## Result

| Measurement | Value |
| --- | ---: |
| Release commit | `e9a6060` |
| Local image ID | `sha256:e212128215726b8280fed0e142ba92496c52b663e20c3df4bb5819d15746448d` |
| Brokers | 3 independent containers |
| Topic | 1 partition, replication factor 3, minimum ISR 2 |
| Producer | Apache Kafka Java 4.3.1, idempotence, `acks=all` |
| Records before kill | 50,000 acknowledged |
| Old leader | broker 1 |
| New leader | broker 2 |
| Total produced | 100,000 acknowledged |
| Total consumed | 100,000 unique |
| Lost | 0 |
| Unexpected duplicates | 0 |
| First post-kill acknowledgement | 9,493 ms |
| Total create/produce/kill/recover/verify time | 10,899 ms |

The verifier encoded one 64-bit sequence number per record, discovered the actual leader through Kafka metadata, waited for brokers 1–3 to enter ISR, disabled the leader container's restart policy, and issued `docker kill`. It kept the same producer instance, waited for a different leader, then consumed from offset zero and checked the complete sequence space.

```powershell
./sbt.bat Test/compile
./scripts/run-failover-demo.ps1 -Image miladsade96/cascade:1.8.0 -Records 100000 -Java "$env:JAVA_HOME\bin\java.exe"
```

## Host and boundary

The run used Docker Desktop 29.8.0 on Windows 11 Pro build 26200 with WSL2 kernel 6.6.87.2, an Intel Core i5-12450HX (8 cores, 12 logical processors), about 21.7 GiB host RAM, and an NVMe system disk. All three containers shared this one laptop, one Docker daemon, one kernel, one physical disk, and one power domain.

`failover_ms` starts immediately before `docker kill` and ends at the first successful callback for a record sent after the kill. It includes controller election, partition metadata commit, client metadata refresh, and retry. It is not a pure election latency. This run does not qualify network partitions, host loss, rack/zone isolation, TLS overhead, sustained churn, or the required 72-hour authenticated multi-tenant soak.

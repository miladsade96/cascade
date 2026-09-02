# Shard-isolated coordinator storage

I am separating changed coordinator shard payloads from the ordered metadata commit journal. This is a staged capacity change: immutable, independently checksummed shard objects and small atomic commit references first; independent shard consensus and service locks remain separate work.

## Safety contract

- Existing clusters keep their inline journal until every voter supports the new feature and format floor.
- I force every referenced object before forcing its commit marker. An interrupted preparation is an orphan, not a committed update.
- One marker publishes every shard of a transaction together, with the existing exact-base validation and quorum rules.
- Recovery must reject a missing, truncated, or corrupt referenced object without truncating a valid commit marker. It may ignore objects that were never published.
- Full snapshots at migration, topology changes, and checkpoints preserve byte-exact coordinator images. Changed shards use separate objects between these checkpoints.
- Reclamation occurs only after a forced, atomically published self-contained checkpoint. It must never delete an object required by any retained journal frame.
- Backup and restore must include the journal and its object directory as one artifact under the existing write barrier.
- Counters distinguish marker bytes, newly forced object bytes, reuse, and reclamation. Smaller markers do not imply smaller total I/O or independent consensus.

## Qualification plan

I require object/record codec and size-bound tests, concurrent preparation, multi-shard atomicity, torn markers, orphan preparation, missing/corrupt object rejection, checkpoint/reclamation restart tests, feature negotiation, cluster failover, exact backup/restore, and the complete regression suite. A long-lived Kafka-client runner will separate connection churn from coordinator work, report latency and actual disk/wire counters, and verify offsets through failover and restart. The existing churn benchmark remains a regression workload.

This work does not qualify multi-day soak, physical power/device loss, independent per-shard consensus, or dedicated-host production capacity. The force/rename contract must still be qualified on the target filesystem and devices.

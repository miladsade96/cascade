# Producer-state recovery runbook

I use this runbook when a broker cannot validate `producer-state.checkpoint`, when producer administration disagrees with the authoritative replica, or after storage maintenance that may have affected a partition directory.

## What the checkpoint protects

Every partition stores `producer-state.checkpoint` beside its segment files. It is a versioned, length-bounded, CRC32C-protected snapshot of the last five accepted batches for each producer. Entries retain the producer ID, epoch, sequence and offset span, record count, timestamp, compression type, and transaction/control flags. Cascade merges this state with surviving magic-v2 headers during startup.

The checkpoint exists because valid retention and compaction can remove the batch that proved a producer sequence. Deleting the checkpoint may therefore make a retry look new and is not a repair. I never edit it, regenerate its CRC, copy one from another partition, or start a second broker against the same directory.

## Triage

1. I keep the broker out of client and replica routing and preserve the startup error.
2. I record the node ID, topic, partition, segment inventory, filesystem/device health, last clean-shutdown state, and the exact source/image revision.
3. I make a read-only forensic copy of the entire partition directory, including hidden replacement/deletion artifacts. I do not copy only the checkpoint.
4. I determine whether an authoritative in-sync replica exists. If it does, I prefer normal fenced replica reset and recovery from that leader. I do not transplant its checkpoint independently of its log.
5. If no authoritative replica exists, I restore the complete broker/partition state from a verified backup generation and follow the backup restore procedure. I do not accept a startup that merely omits the corrupt checkpoint.

Cascade treats a checksum, version, size, ordering, or metadata violation as corruption and stops recovery. This is intentional: silently discarding the file would weaken duplicate detection after cleaned segments.

## Replica divergence

An authoritative replica truncation first writes a producer checkpoint containing only metadata below the accepted boundary, then truncates the suffix and rebuilds its indexes. A full replica reset writes an empty producer checkpoint before removing local segments. After the leader streams the committed prefix, normal recovery rebuilds producer state from the received batches. I use only the broker's fenced recovery path for these operations.

## Return-to-service checks

Before returning the broker to service I require all of the following:

- startup and readiness complete without checkpoint or record-batch errors;
- the partition log start, end, and high-watermark values agree with the authoritative replica;
- `DescribeProducers` reports the expected producer IDs, epochs, and last sequences;
- a controlled duplicate retry returns its original offset and does not add a record;
- the next contiguous sequence is accepted and a deliberate gap is rejected;
- `read_committed` exposes committed values, hides aborted/open values, and reports the expected last stable offset; and
- the broker can complete one clean shutdown/restart with the same results.

I archive the forensic copy, relevant structured logs, validation output, device diagnostics, and incident decision. Repeated corruption on a host is a storage, memory, filesystem, or power-path incident; I do not treat repeated restore as remediation.

## Planned drills

For release qualification I exercise compaction and size retention before restart, corrupt a copied checkpoint and require fail-closed startup, truncate a divergent replica suffix, and run Kafka-client commit/abort plus duplicate/sequence validation. Physical device-loss and power-cut testing remains a separate production gate.

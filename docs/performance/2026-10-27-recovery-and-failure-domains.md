# 1.16.0 recovery and failure-domain qualification

On 2026-10-27 I qualified the exact published `miladsade96/cascade:1.16.0` index `sha256:85c727ab007c3f5fbb10f0f616e27f76e5fbbea989ea90caeacc88942be30883`. The runtime reports source revision `e7c535a18f675ac6a10fe25c65a4355c782396a7` and runs as UID/GID 65532.

## External object-store restore

I produced 250 deterministic records with synchronous flushing, consumed the exact ledger, and committed group offset 250. After a clean broker stop I created and verified the checksummed backup, copied it into digest-pinned MinIO object storage, and confirmed the remote manifest. I then deleted both the source-data volume and local backup-staging volume before downloading and verifying the remote generation, restoring into a new volume, starting 1.16.0, and reading from Kafka again.

| Measurement | Result |
| --- | ---: |
| Acknowledged / recovered | 250 / 250 |
| Last offset | 249 |
| Committed offset | 250 |
| Record loss | 0 |
| Observed RPO | 0 records / 0 ms |
| Last acknowledgement to durable object copy | 4,658 ms |
| RTO through exact Kafka validation | 16,434 ms |
| Backup | 931 ms |
| Upload | 655 ms |
| Download | 647 ms |
| Restore | 941 ms |
| Broker readiness | 6,669 ms |
| Exact validation | 7,289 ms |

The recovered SHA-256 ledger was `583b86bd54de2b39fc5fccb9fdfbc3f30c8db6a74a3d08b06c869b5b87e7acc9`. The object store had a separate container and persistent volume but shared the same physical Docker Desktop host. I count this as external-to-cluster transfer and restore evidence, not off-host, full-cluster, encrypted-transfer, multi-terabyte, or physical-loss qualification.

## Java compatibility on the final registry image

I prefetched the pinned Maven graphs with bounded retries, then ran the compatibility phase offline. Apache Kafka Java clients 3.9.1, 4.0.0, 4.1.1, 4.2.0, and 4.3.1 each produced and consumed 25/25 exact records and verified committed offset 25. Every prefetch completed on the first attempt. This closes the earlier final-image evidence gap caused by Maven Central connectivity, while keeping dependency availability separate from protocol compatibility.

## Independent-host gate

I added a fail-closed runner that requires three Docker contexts, three routable host addresses, and three distinct Linux engine IDs. It exercises an RF=3 ledger through one-host network isolation, ISR recovery, simultaneous active-controller and second-broker loss, minority write rejection, quorum recovery, 1.15.0-to-1.16.0 rolling replacement across machines, and a full three-host restart.

This workstation exposes `default` and `desktop-linux`, but both resolve to the same Engine ID `4007c204-dd69-4c0c-a291-dba3ab25a798`. The preflight correctly stopped before pulling images, creating volumes, or starting brokers. The runner is implemented and its exact client contracts pass the full suite, but the real three-independent-host result is still open until three reachable engines are supplied.

## Regression result

The complete Java 21 source suite passed **696/696** tests with zero failures or errors in **191 seconds**. That result includes the new disaster-recovery and multi-host ledger contracts. It does not convert the blocked multi-host preflight into a passing host-failure campaign.

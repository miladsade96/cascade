# Adjacent image rolling qualification — 2026-10-21

I ran the real 1.14.0-to-1.15.0 image boundary on Windows with Docker Desktop 29.8.2 and Eclipse Adoptium Java 21.0.11. This is a correctness campaign, not a throughput benchmark.

## Inputs

| Item | Value |
| --- | --- |
| Baseline | `miladsade96/cascade:1.14.0` |
| Baseline image ID | `sha256:ba0887a11a3115fc5ecd803d7fabf72ed0d72485d9290740ae68c7b00d911a18` |
| Baseline revision | `db57f789269053e9ce0f4a5cddc243685b61304b` |
| Candidate | `miladsade96/cascade:1.15.0` |
| Candidate qualification revision | `126d309d5f7d1ec1641575507f15b554654acd8b` |
| Topology | Three brokers, RF=3, minimum ISR 2, synchronous flush, one persistent volume per voter |
| Container policy | UID/GID 65532, read-only root, capability drop, `no-new-privileges`, noexec temporary filesystem |

Both tags resolved to different immutable SHA-256 image IDs. The gate also checked the exact version/revision/creation labels and image health check before creating the cluster.

## Result

| Check | Result |
| --- | --- |
| Phases | 10/10 passed |
| Exact records | 1,200/1,200 |
| Exact committed offset | 1,200 |
| Kafka `record-retry-total` | 100 |
| Baseline leader replaced | Node 1 |
| Pre-upgrade rollback | Passed |
| Post-upgrade compatible downgrade | Passed |
| All-candidate restart recovery | Passed |
| Broker protocol errors | 0 |
| Elapsed time | 96.321 seconds |

One idempotent producer remained open across the complete run. The runner upgraded a non-leader, rolled it back, upgraded it again, upgraded the other non-leader, paused the old leader while 1,000 records were submitted, waited for a measured Kafka retry, and hard-replaced that leader. It then rejoined one 1.14.0 node after the cluster had run 1.15.0, restored the candidate, restarted every candidate container from its original volume, and compared every value, offset, and committed group position.

## Defects found while qualifying the gate

The first image inspection used quoted Go-template label keys. Windows `ProcessBuilder` removed those embedded quotes before Docker parsed the template. I changed the command to return the label map as JSON and made the parser reject missing, duplicated, malformed, or escaped release fields.

The first transition submitted 1,000 records but the local cluster acknowledged them before Docker removed the leader, so no retry was measured. I did not weaken the requirement. The runner now pauses the old leader before the wave, waits until Kafka reports a real idempotent retry, and only then unpauses and hard-replaces it. The passing run recorded 100 retries.

A later final-image run reached that hard replacement and found a broker recovery defect. Replica truncation could persist both an older checkpoint entry and the identical retained segment metadata. CRC and all fields were valid, but restart rejected the duplicate before the broker became ready. Producer-state persistence and loading now canonicalize only exact duplicates by producer ID and base offset; a different epoch, sequence, range, flag, timestamp, or compression value at the same identity still fails closed. A crafted legacy-checkpoint regression proves both outcomes.

## Boundaries

This run qualifies these two local Linux/amd64 images on one Docker Desktop host. It does not establish a capacity envelope, ARM64 compatibility, arbitrary network impairment, multi-host clock or storage behavior, every historic release pair, or safety under physical device/power loss. The final release image still needs the packaged runtime, five-language, Java-version, and authenticated zero-finding security gates. Registry publication and digest verification are separate operations.

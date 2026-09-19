# Leader-failover demo

I use this demo as short, reproducible evidence of the same failure boundary covered by the larger fault suite.

## What it proves

The runner starts the 1.8.0 three-broker Compose topology with replication factor 3 and minimum ISR 2. A real Kafka Java producer uses idempotence and `acks=all`. The verifier discovers the actual partition leader, disables that container's restart policy, sends `docker kill`, and continues using the same producer instance.

After a surviving ISR replica becomes leader, a consumer reads the partition from its beginning and verifies every encoded sequence number exactly once. The result includes old/new leader IDs, acknowledged and consumed counts, lost records, unexpected duplicates, first post-failure acknowledgement latency, and total elapsed time.

## Run it

I compile the test tools once before recording so sbt startup is not part of the 60–90 second demo:

```powershell
$env:JAVA_HOME='C:\Program Files\Eclipse Adoptium\jdk-21'
$env:Path="$env:JAVA_HOME\bin;$env:Path"
./sbt.bat Test/compile
./scripts/run-failover-demo.ps1 -Image miladsade96/cascade:1.8.0 -Records 100000 -Java "$env:JAVA_HOME\bin\java.exe"
```

The script requires the exact image locally, uses an isolated Compose project, waits for all three health checks, writes JSON below `artifacts/`, and removes its containers and volumes in `finally`.

## Recording checklist

1. Show the image ID and 1.8.0 revision label.
2. Start the script without trimming warnings that affect the result.
3. Show the discovered old leader and acknowledged pre-failure count.
4. Show the killed container and newly elected leader.
5. Show final JSON with `lost=0` and `unexpected_duplicates=0`.
6. Keep the full unedited terminal capture with the JSON evidence.

The measured `failover_ms` is the interval from `docker kill` invocation to the first successful callback for a post-failure record. It includes client metadata refresh and retry. It is not the controller's internal election duration.

The latest recorded run killed broker 1 at 50,000 acknowledgements, elected broker 2, acknowledged 100,000 records, and consumed all 100,000 with zero loss and zero unexpected duplicates. Its measured recovery was 9,395 ms. I recorded the exact candidate image and limitations in [the dated failover report](performance/2026-09-19-failover-demo.md).

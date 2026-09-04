# Cascade

I build Cascade in pure Scala 3 as a Kafka-wire-compatible streaming broker. Existing Kafka clients connect without a Cascade-specific SDK. Compatibility is an explicit API/version subset, not full Apache Kafka parity.

## Image

`miladsade96/cascade:1.3.0` targets **Linux/amd64**. It includes immutable coordinator shard storage, bounded offset batching, cached snapshots, group-indexed offset reads, and ownership-aware consumer expiry, alongside replication, transactions, TLS/SASL/ACLs, quotas, and operations endpoints.

I use a distroless Debian base, a module-limited Java 21 runtime, UID/GID 65532, and a built-in readiness probe. The image has no shell. Data belongs in `/var/lib/cascade`; Kafka listens on port 9092 and operations defaults to loopback port 9404.

```bash
docker pull miladsade96/cascade:1.3.0
docker volume create cascade-data
docker run -d --name cascade --read-only \
  --tmpfs /tmp:size=64m,mode=1777,nosuid,nodev,noexec \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --memory 2g -p 9092:9092 \
  -v cascade-data:/var/lib/cascade \
  miladsade96/cascade:1.3.0
```

This example advertises `localhost` for local development. For remote clients I pass `--advertised-host` with a reachable DNS name and configure TLS/SASL/ACLs before exposing the Kafka port. I do not expose the operations port without authentication and a TLS/mTLS proxy. `docker stop --timeout 120 cascade` gives the broker time to flush; removing a container does not replace a data backup.

## Scope

Cascade is **not yet a production-grade Kafka replacement**. Shared coordinator consensus/locks, broader API coverage, multi-day soak, dedicated-host capacity, physical power/device loss, and some published-image rolling-upgrade boundaries remain open. Snapshot development passed 445 full-suite tests, 79 Linux tests, five external client languages, and exact one-million-record checks; release-image results and immutable digests are kept separately in the repository's 1.3.0 release notes.

- [Source and API compatibility](https://github.com/miladsade96/cascade)
- [Container guide](https://github.com/miladsade96/cascade/blob/main/docs/containers.md)
- [Release notes](https://github.com/miladsade96/cascade/blob/main/docs/releases/1.3.0.md)
- [Production-readiness checklist](https://github.com/miladsade96/cascade/blob/main/docs/production-readiness.md)

Apache License 2.0. This file is the version-controlled Docker Hub description; publishing the image does not automatically update the repository description on Docker Hub.

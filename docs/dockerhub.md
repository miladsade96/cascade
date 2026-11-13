# Cascade

I build Cascade in pure Scala 3 as a Kafka-wire-compatible streaming broker. Existing Kafka clients connect without a Cascade-specific SDK. Compatibility is an explicit API/version subset, not full Apache Kafka parity.

## Supported image

`miladsade96/cascade:1.16.0` is published for **Linux/amd64** at immutable index digest `sha256:85c727ab007c3f5fbb10f0f616e27f76e5fbbea989ea90caeacc88942be30883`. Docker Hub reports **no vulnerabilities** for this tag. The image contains patched musl `1.2.6-r6`, a minimized Java 21 runtime, and the complete 1.16.0 broker.

I use a minimal Alpine/musl filesystem without glibc or OpenSSL, a module-limited Java 21 runtime, UID/GID 65532, and a built-in readiness probe. TLS is supplied by Java. The image has no shell. Data belongs in `/var/lib/cascade`; Kafka listens on port 9092 and operations defaults to loopback port 9404.

```bash
docker pull miladsade96/cascade:1.16.0
docker volume create cascade-data
docker run -d --name cascade --read-only \
  --tmpfs /tmp:size=64m,mode=1777,nosuid,nodev,noexec \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --memory 2g -p 9092:9092 \
  -v cascade-data:/var/lib/cascade \
  miladsade96/cascade:1.16.0
```

This example advertises `localhost` for local development. For remote clients I pass `--advertised-host` with a reachable DNS name and configure TLS/SASL/ACLs before exposing the Kafka port. I do not expose the operations port without authentication and a TLS/mTLS proxy. `docker stop --timeout 120 cascade` gives the broker time to flush; removing a container does not replace a data backup.

## Deprecated vulnerable tags

I do not deploy `1.12.0`, `1.13.0`, `1.14.0`, or `1.15.0`. I retain these immutable tags only for historical and rolling-upgrade reproduction. They contain Alpine musl `1.2.6-r2`, which Docker Scout reports as affected by `CVE-2026-107891`. I will not overwrite immutable historical bytes; I upgrade deployments to `1.16.0` instead.

## Qualification and scope

The exact `1.16.0` registry image passes the packaged runtime gate, five external client languages, Kafka Java clients 3.9.1 through 4.3.1, the 1.15.0-to-1.16.0 rolling campaign, and a zero-finding security scan. I also restored 250/250 exact records and committed offset 250 from an external S3-compatible object store after deleting the original source and staging volumes.

Cascade is **not yet a production-grade Kafka replacement**. The actual 72-hour authenticated soak, three-independent-host failure-domain run, dedicated-host capacity envelope, physical power/device loss, recurring encrypted off-host full-cluster restore drills, and future adjacent published-image boundaries remain open. Exact qualification, current scan results, publication status, and immutable digests are in the release notes. A clean scan is point-in-time evidence, not a permanent security guarantee.

- [Source and API compatibility](https://github.com/miladsade96/cascade)
- [Container guide](https://github.com/miladsade96/cascade/blob/main/docs/containers.md)
- [Release notes](https://github.com/miladsade96/cascade/blob/main/docs/releases/1.16.0.md)
- [Release security policy](https://github.com/miladsade96/cascade/blob/main/docs/container-security.md)
- [Production-readiness checklist](https://github.com/miladsade96/cascade/blob/main/docs/production-readiness.md)

Apache License 2.0. This file is the version-controlled Docker Hub description; publishing the image does not automatically update the repository description on Docker Hub.

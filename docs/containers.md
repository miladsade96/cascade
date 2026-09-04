# Container deployment

I publish Cascade as `miladsade96/cascade` and keep the image contract deliberately small: the entry point is the broker, every broker option remains a normal command argument, `/var/lib/cascade` is the persistent data root, port 9092 is the Kafka listener, and port 9404 is reserved for operations.

## Image design

I build the application and its Scala runtime dependencies in a JDK 21 stage, create a module-limited Java runtime with `jlink`, and copy only that runtime plus three application/runtime jars into a distroless Debian image. The final image:

- runs as numeric UID and GID 65532 rather than root;
- has no shell or package manager;
- starts the JVM directly so SIGTERM reaches Cascade's shutdown hook;
- uses cgroup-aware JVM percentage limits and exits on an out-of-memory error;
- includes an internal Java readiness probe against `/ready`;
- declares `/var/lib/cascade` as the durable volume;
- carries OCI source, version, revision, creation-time, and license labels; and
- uses a pinned Debian 13 no-OpenSSL base, with Java TLS supplied by the JDK; and
- is qualified and published for `linux/amd64` with SBOM and provenance attestations. ARM64 publication remains disabled until equivalent runtime qualification is added.

The default operations listener stays on `127.0.0.1` inside the container. Docker can therefore evaluate readiness without exposing an unauthenticated HTTP endpoint. When I need remote metrics, I bind operations explicitly to `0.0.0.0`, mount a token file, set `CASCADE_HEALTHCHECK_TOKEN_FILE` to the same file, and put TLS or mTLS in front of port 9404.

## Pull and run one broker

The current image target is `miladsade96/cascade:1.3.1` for `linux/amd64`. I record build identity, qualification, publication status, and the immutable registry digest in the [1.3.1 release notes](releases/1.3.1.md). Deployment examples are aligned to this version. I do not retag older releases or move `latest` as part of this publication.

Release 1.3.0 includes [shard-object storage](shard-storage.md), [bounded offset batching](offset-batching.md), and [cached coordinator snapshots](coordinator-snapshots.md). To reproduce an image I check out its recorded build revision and retain the Dockerfile's pinned base digests. This image does not by itself qualify every older published-image upgrade boundary.

`deploy/VERSION` records that deployment pin separately from the source `VERSION`. Tests permit a different pin only for `-SNAPSHOT` development; a release must align both files, the manifests, and the documented image commands.

The historical snapshot milestones ran staged jars inside a full JDK container. For the release I use `scripts/qualify-staged-clients.ps1 -BrokerImage miladsade96/cascade:1.3.1 -ExpectedVersion 1.3.1` to test the actual distroless image, with all five pinned clients and Java restart recovery. It checks the image's own user, entry point, JVM configuration, and health probe rather than substituting a JDK runtime. I also run `scripts/qualify-image-runtime.ps1` against the packaged JVM and broker for TLS/SASL and secure-peer regression tests.

The 1.3.0 release passed its functional tests, but its original vulnerability scan failed. A subsequent scan found 21 vulnerabilities in its Debian 12 libc/OpenSSL packages. The 1.3.1 patch changes the base and adds the [fail-closed security gate](container-security.md); functional success is no longer sufficient for publication. Historical sizes and test counts are retained in each release's own notes.

I retain the older 1.2.0 image's size, ID, and client checks only in its [historical qualification report](performance/2026-09-02-incremental-coordinator.md); those numbers are not measurements of 1.3.0.

```bash
docker pull miladsade96/cascade:1.3.1
docker volume create cascade-data
docker run --detach \
  --name cascade \
  --init \
  --read-only \
  --tmpfs /tmp:size=64m,mode=1777,nosuid,nodev,noexec \
  --cap-drop ALL \
  --security-opt no-new-privileges:true \
  --ulimit nofile=100000:100000 \
  --memory 4g \
  --publish 9092:9092 \
  --mount type=volume,source=cascade-data,target=/var/lib/cascade \
  miladsade96/cascade:1.3.1 \
  --host 0.0.0.0 \
  --port 9092 \
  --advertised-host localhost \
  --advertised-port 9092 \
  --data-dir /var/lib/cascade \
  --operations-port 9404
```

I wait for `docker inspect --format '{{.State.Health.Status}}' cascade` to report `healthy`, then connect a Kafka client to `localhost:9092`. I replace `localhost` with a DNS name or address reachable by every client in a remote deployment. The advertised address is client-facing; Docker service names belong only in `--cluster-nodes`.

I stop with enough time for dirty segments and journals to be forced:

```bash
docker stop --timeout 120 cascade
docker rm cascade
```

Removing the container does not remove `cascade-data`. I delete that named volume only when I intentionally want to destroy the broker's durable state.

## Docker Compose

For one development broker I use:

```bash
docker compose up --build --detach
docker compose ps
docker compose logs --follow broker
```

`compose.yaml` builds `cascade:local`, publishes port 9092, enables the internal health check, drops Linux capabilities, uses a read-only root filesystem, and persists data in `cascade-data`. `docker compose down` performs a graceful stop and preserves the volume.

For a three-broker development cluster I use:

```bash
docker compose -f compose.cluster.yaml up --build --detach
docker compose -f compose.cluster.yaml ps
```

The brokers are available to host clients at `localhost:19092`, `localhost:19093`, and `localhost:19094`. The three development processes share one Docker network namespace so those same localhost endpoints work for peer discovery and for clients outside Docker, while their processes and named data volumes remain separate. The file assigns a replication factor of three and requires two in-sync replicas. This is a local qualification topology, not a substitute for a production orchestrator, separate network and machine failure domains, TLS, resource reservations, or durable storage classes.

On Linux, a bind-mounted data directory must be writable by UID 65532. I prefer a named volume unless I have already provisioned and permissioned the host directory. I also size the container memory above the expected heap plus direct buffers, thread stacks, native TLS state, and page cache; `MaxRAMPercentage` governs only the JVM heap.

## Kubernetes

I keep the production-oriented three-broker base in `deploy/kubernetes`. It uses three one-replica StatefulSets so every broker has a stable node ID, DNS identity, TLS secret, and retained PVC. The base also applies required host anti-affinity, preferred zone spreading, a two-of-three disruption budget, `OnDelete` updates, 180-second termination grace, non-root/seccomp/read-only security contexts, CPU and memory reservations, startup/readiness/liveness probes, and default-deny network policy.

Before I apply it, I create these secrets in the `cascade` namespace:

- `cascade-node-1-tls`, `cascade-node-2-tls`, and `cascade-node-3-tls`, each with `keystore.p12`, `keystore.password`, and `key.password`;
- `cascade-cluster-trust` with `truststore.p12` and `truststore.password`;
- `cascade-client-auth` with `scram.conf`; and
- `cascade-operations` with a random `token` of at least 32 characters.

I replace the example peer certificate subjects in `deploy/kubernetes/peer-policy.yaml`, review the storage class and 100 GiB PVC request, pin the image to a verified immutable digest, label client and monitoring namespaces for the NetworkPolicy, then render and apply:

```bash
kubectl kustomize deploy/kubernetes > cascade-rendered.yaml
kubectl apply --server-side --filename cascade-rendered.yaml
kubectl --namespace cascade rollout status statefulset/cascade-1
kubectl --namespace cascade rollout status statefulset/cascade-2
kubectl --namespace cascade rollout status statefulset/cascade-3
```

The operations pack includes a bearer-authenticated ServiceMonitor, Prometheus rules, and a Grafana dashboard ConfigMap. It expects the Prometheus Operator CRDs and a dashboard sidecar that discovers `grafana_dashboard=1`. I keep Alertmanager receivers and credentials outside the repository.

For a rolling change I delete one pod at a time because the StatefulSets use `OnDelete`. I wait for the replacement to become ready, confirm it is synchronized and unfenced, and verify `acks=all` traffic before touching the next broker. The pinned 1.0.0-to-1.1.0 process matrix now qualifies that exact release boundary, including rollback before activation and safe downgrade rejection afterward. I follow the [rolling upgrade runbook](rolling-upgrades.md) and require a new archived campaign for every later adjacent release pair.

## Authenticated remote operations

I create the token outside the image, restrict its permissions, and mount it read-only. I then add these environment and broker settings to my deployment:

```yaml
ports:
  - "9404:9404"
environment:
  CASCADE_HEALTHCHECK_TOKEN_FILE: /run/secrets/operations-token
volumes:
  - ./secrets/operations.token:/run/secrets/operations-token:ro
command:
  - --host
  - 0.0.0.0
  - --port
  - "9092"
  - --advertised-host
  - broker.example.com
  - --data-dir
  - /var/lib/cascade
  - --operations-host
  - 0.0.0.0
  - --operations-port
  - "9404"
  - --operations-token-file
  - /run/secrets/operations-token
```

The built-in operations transport is HTTP. I never publish port 9404 beyond a protected network without a TLS/mTLS reverse proxy or service mesh.

## Build and qualify locally

```bash
docker build --check .
docker build --tag cascade:local .
docker compose up --detach
./sbt "Test/runMain cascade.e2e.ExternalBrokerSmokeTest localhost:9092"
docker compose down
```

The smoke test uses the official Kafka Java client to discover the broker, initialize a non-transactional idempotent producer, produce 25 exact records with `acks=all`, consume them from the beginning, and compare every key and value. My GitHub workflow restarts the container and verifies the same records from the persistent volume. Publication also requires the complete Scala suite, packaged TLS/SASL tests, security-gate regression tests, and a successful zero-finding image scan.

On 2026-08-30 I built both `linux/amd64` and `linux/arm64` targets and scanned the final local amd64 image with Docker Scout. It indexed 15 packages and reported zero critical, high, medium, or low vulnerabilities. That is a point-in-time result, so I scan every release again instead of treating it as a permanent property of the base image.

## Publish to Docker Hub

For a manual release I authenticate with a scoped Docker Hub access token and never put that token in the repository or command history:

```bash
docker login --username YOUR_DOCKERHUB_USERNAME
docker buildx build \
  --platform linux/amd64 \
  --build-arg VERSION=1.3.1 \
  --build-arg REVISION=GIT_COMMIT_SHA \
  --tag YOUR_DOCKERHUB_USERNAME/cascade:1.3.1 \
  --provenance=mode=max \
  --sbom=true \
  --load \
  .
# Complete runtime/client/cluster qualification before this release gate.
node scripts/qualify-image-security.mjs YOUR_DOCKERHUB_USERNAME/cascade:1.3.1 1.3.1 artifacts/security-new-run
# Only after the gate exits successfully and result.json matches the image ID:
docker push YOUR_DOCKERHUB_USERNAME/cascade:1.3.1
docker buildx imagetools inspect YOUR_DOCKERHUB_USERNAME/cascade:1.3.1
```

The automated path is `.github/workflows/container.yml`. I configure the GitHub repository variable `DOCKERHUB_USERNAME` and the secret `DOCKERHUB_TOKEN`. Pull requests and `main` pushes qualify without publishing. A matching `v*` tag or manual run publishes only the explicit release version after all gates pass. The publish job loads the already-tested image archive, scans again, and checks the remote digest; it never rebuilds, adds ARM64, or moves `latest`. Docker's containerd image store is required to preserve attestations through local loading/export.

I treat image tags as release pointers, not backups. I deploy immutable image digests, retain the matching source tag and provenance, scan the resulting SBOM, and practice restoring the broker data volume independently of the image.

# Container security release gate

I do not publish when vulnerability scanning fails. Unit tests, non-root execution, a small image, and an SBOM do not replace a completed vulnerability assessment.

## Policy

`scripts/qualify-image-security.mjs` accepts a local image, its release version, and a new evidence directory. It requires Node 22 or newer, Docker with the containerd image store, Docker Scout, and `tar`. Linux CI installs Scout 1.24.0 from its official release archive with a checked-in SHA-256 checksum. Scanner download, authentication, network, or database errors block release, including on untrusted/fork CI runs where credentials may be unavailable.

The gate inspects the immutable image ID, exports it locally, verifies the SHA-256/size chain of its OCI metadata, and reads the exact SPDX attestation attached to the Linux/amd64 runtime. It validates the runtime user/version, provenance subject, and inventory entries for the application, Scala, JDK, libc, and certificates. OpenSSL packages are forbidden in the no-OpenSSL image. It never extracts arbitrary archive paths onto the filesystem.

Docker Scout scans this complete SPDX inventory with `--exit-code`, without severity, fixed-version, base-image, suppression, or VEX exclusion filters. Every reported vulnerability blocks release, including low and unspecified severity. Nonzero exits, missing or malformed SARIF, incomplete results, execution errors, and mismatched image identities also block release. There is no exception list. I must fix a finding or explicitly change and review the policy; I do not silently mark it harmless.

Scanning the image's existing attestation avoids downloading a second Java package-discovery database. It does not skip Java: the inventory must contain the packaged OpenJDK and Scala dependencies. This approach still relies on BuildKit's inventory completeness and the scanner's current advisory database. It does not perform source-code analysis or establish exploitability.

```bash
node --test scripts/qualify-image-security.test.mjs
node scripts/qualify-image-security.mjs miladsade96/cascade:1.3.1 1.3.1 artifacts/security-unique-run
```

The evidence directory must not already exist. `result.json` is written only after a successful scan and records the image/index digest, runtime manifest, release version, scanner version, scan time, and zero findings. The directory also preserves the image archive, SPDX inventory, full SARIF report, and scanner log. A failed attempt cannot reuse a previous passing report.

## Release workflow

The GitHub container workflow runs the full Scala suite, gate regression tests, actual-image startup/restart checks, packaged TLS/SASL/security suites, and a blocking vulnerability scan. It uploads the image archive only after these checks pass. The publication job downloads that archive, scans it again, verifies its identity, and pushes only the exact release-version tag. There is no second build, automatic `latest` update, or unqualified ARM64 publication.

Locally I additionally run the five-language matrix and three-broker replication/restart campaign. The Windows helpers are `qualify-staged-clients.ps1 -BrokerImage ...` and `qualify-image-runtime.ps1 -BrokerImage ...`. The runtime helper loads broker/Scala classes from `/opt/cascade/lib/*`, excludes host main classes, and mounts only test classes and client dependencies. The image retains its non-root user and read-only root filesystem; test listeners remain inside its container network namespace.

I retain published digests and date each report. New advisories can affect an unchanged image later, so a clean release scan is not a permanent guarantee. The 1.3.0 post-publication findings are the reason for this policy.

Sources: [Distroless base contents](https://github.com/GoogleContainerTools/distroless/blob/main/base/README.md), [Docker Scout CLI](https://docs.docker.com/reference/cli/docker/scout/cves/), [containerd image store in GitHub Actions](https://docs.docker.com/build/ci/github-actions/multi-platform/).

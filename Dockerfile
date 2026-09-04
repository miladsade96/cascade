# syntax=docker/dockerfile:1.7

ARG JDK_IMAGE=eclipse-temurin:21-jdk-jammy@sha256:ce5767b7222312d42395f5bab033cd91f09e44032a2f21bdfd7b5b912dbe1e77
ARG RUNTIME_JDK_IMAGE=eclipse-temurin:21-jdk-alpine@sha256:6ea5548706b60ac0a602eaf48af74792cbab012d90e811ca8db6184b16b5c3d6
ARG RUNTIME_IMAGE=alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b

FROM --platform=$BUILDPLATFORM ${JDK_IMAGE} AS application-build

WORKDIR /workspace
COPY sbt build.sbt VERSION ./
COPY project/build.properties project/build.properties
RUN chmod 0755 sbt && ./sbt update

COPY src/main src/main
RUN ./sbt stage

FROM ${RUNTIME_JDK_IMAGE} AS java-runtime-build

RUN "$JAVA_HOME/bin/jlink" \
      --add-modules java.base,java.net.http,jdk.crypto.ec,jdk.httpserver,jdk.unsupported \
      --compress=zip-6 \
      --no-header-files \
      --no-man-pages \
      --strip-debug \
      --output /opt/cascade-java && \
    install -d -o 65532 -g 65532 /image/var/lib/cascade

FROM ${RUNTIME_IMAGE} AS runtime-base-build

# Install only runtime packages into an empty root, retaining the real APK inventory.
# No shell, package manager, glibc, or OpenSSL is copied into the final image.
RUN apk --root /runtime --initdb --no-cache --no-scripts \
      --repositories-file /etc/apk/repositories add \
      alpine-baselayout-data=3.7.2-r1 musl=1.2.6-r2 libstdc++=15.2.0-r5 \
      ca-certificates-bundle=20260611-r0 tzdata=2026c-r0 && \
    cp /etc/alpine-release /etc/os-release /runtime/etc/ && \
    mkdir -p /runtime/tmp /runtime/var/lib/cascade && \
    chmod 1777 /runtime/tmp && \
    chown 65532:65532 /runtime/var/lib/cascade

COPY deploy/container/passwd /runtime/etc/passwd
COPY deploy/container/group /runtime/etc/group

FROM scratch AS runtime

ARG VERSION=1.3.1
ARG REVISION=unknown
ARG CREATED=unknown

LABEL org.opencontainers.image.title="Cascade" \
      org.opencontainers.image.description="A high-performance Kafka wire-compatible broker written in Scala" \
      org.opencontainers.image.url="https://github.com/miladsade96/cascade" \
      org.opencontainers.image.source="https://github.com/miladsade96/cascade" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.created="${CREATED}" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV JAVA_HOME=/opt/java \
    PATH=/opt/java/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    JAVA_TOOL_OPTIONS="-XX:InitialRAMPercentage=10.0 -XX:MaxRAMPercentage=75.0 -XX:+UseG1GC -XX:+ExitOnOutOfMemoryError -Dfile.encoding=UTF-8" \
    CASCADE_OPERATIONS_PORT=9404 \
    CASCADE_HEALTHCHECK_HOST=127.0.0.1 \
    CASCADE_HEALTHCHECK_TIMEOUT_MS=2000

COPY --from=runtime-base-build /runtime/ /
COPY --from=java-runtime-build --chown=65532:65532 /opt/cascade-java /opt/java
COPY --from=application-build --chown=65532:65532 /workspace/target/docker-stage/lib /opt/cascade/lib
COPY --from=java-runtime-build --chown=65532:65532 /image/var/lib/cascade /var/lib/cascade

USER 65532:65532
WORKDIR /var/lib/cascade

VOLUME ["/var/lib/cascade"]
EXPOSE 9092 9404
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 \
  CMD ["/opt/java/bin/java", "-cp", "/opt/cascade/lib/*", "cascade.operations.ContainerHealthCheck"]

ENTRYPOINT ["/opt/java/bin/java", "-cp", "/opt/cascade/lib/*", "cascade.Main"]
CMD ["--host", "0.0.0.0", "--port", "9092", "--advertised-host", "localhost", "--data-dir", "/var/lib/cascade", "--operations-port", "9404"]

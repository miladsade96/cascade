# syntax=docker/dockerfile:1.7

ARG JDK_IMAGE=eclipse-temurin:21-jdk-jammy@sha256:ce5767b7222312d42395f5bab033cd91f09e44032a2f21bdfd7b5b912dbe1e77
ARG RUNTIME_IMAGE=gcr.io/distroless/base-nossl-debian13:nonroot@sha256:5cab74e7f8a5e7c5f1c8a9e6268b1f352f053c36c656f493308340bcecbc636c

FROM --platform=$BUILDPLATFORM ${JDK_IMAGE} AS application-build

WORKDIR /workspace
COPY sbt build.sbt VERSION ./
COPY project/build.properties project/build.properties
RUN chmod 0755 sbt && ./sbt update

COPY src/main src/main
RUN ./sbt stage

FROM ${JDK_IMAGE} AS java-runtime-build

RUN "$JAVA_HOME/bin/jlink" \
      --add-modules java.base,java.net.http,jdk.crypto.ec,jdk.httpserver,jdk.unsupported \
      --compress=zip-6 \
      --no-header-files \
      --no-man-pages \
      --strip-debug \
      --output /opt/cascade-java && \
    install -d -o 65532 -g 65532 /image/var/lib/cascade

FROM ${RUNTIME_IMAGE} AS runtime

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

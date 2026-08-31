package cascade.operations

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class ContainerHealthCheckSuite extends munit.FunSuite:
  test("parses bounded container health-check settings") {
    val parsed = ContainerHealthCheck.parseEnvironment(
      Map(
        "CASCADE_HEALTHCHECK_HOST" -> "broker",
        "CASCADE_OPERATIONS_PORT" -> "19404",
        "CASCADE_HEALTHCHECK_TIMEOUT_MS" -> "3500",
        "CASCADE_HEALTHCHECK_TOKEN_FILE" -> "/run/secrets/operations-token"
      )
    )
    assertEquals(
      parsed,
      Right(ContainerHealthCheckConfig("broker", 19404, 3500, Some(java.nio.file.Paths.get("/run/secrets/operations-token"))))
    )
    assert(ContainerHealthCheck.parseEnvironment(Map("CASCADE_OPERATIONS_PORT" -> "0")).isLeft)
    assert(ContainerHealthCheck.parseEnvironment(Map("CASCADE_HEALTHCHECK_TIMEOUT_MS" -> "99")).isLeft)
    assertEquals(ContainerHealthCheck.probePath("/live"), Right("/live"))
    assert(ContainerHealthCheck.probePath("/metrics").isLeft)
  }

  test("probes public and bearer-protected readiness endpoints") {
    val publicServer = server(None)
    publicServer.start()
    try
      assertEquals(
        ContainerHealthCheck.probe(ContainerHealthCheckConfig("127.0.0.1", publicServer.boundPort, 2000, None)),
        Right(())
      )
    finally publicServer.close()

    val token = "container-health-token-with-at-least-32-characters"
    val tokenFile = Files.createTempFile("cascade-container-health", ".token")
    Files.writeString(tokenFile, token, StandardCharsets.UTF_8)
    val protectedServer = server(Some(token))
    protectedServer.start()
    try
      val config = ContainerHealthCheckConfig("127.0.0.1", protectedServer.boundPort, 2000, Some(tokenFile))
      assertEquals(ContainerHealthCheck.probe(config), Right(()))
      assert(ContainerHealthCheck.probe(config.copy(tokenFile = None)).isLeft)
    finally
      protectedServer.close()
      Files.deleteIfExists(tokenFile): Unit
  }

  private def server(token: Option[String]): OperationsServer =
    val metrics = BrokerMetricsSnapshot(
      1, 0L, true, false, 1, false, 0, 0, 0, 0L, 0, 0L, 0, 0L, 0L, 0L,
      TrafficSnapshot(0L, 0L, 0L, 0L, 0L, 0L), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L,
      1024L, 1024L, 0L, 1024L
    )
    OperationsServer(
      OperationsConfig(port = Some(0), authenticationToken = token, logToStderr = false),
      () => metrics,
      () => BrokerHealth(live = true, ready = true, checks = Vector(HealthCheck("broker_running", healthy = true, detail = "running")))
    )

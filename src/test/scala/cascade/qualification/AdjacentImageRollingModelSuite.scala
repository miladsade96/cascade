package cascade.qualification

import java.nio.file.Paths
import java.time.Duration
import munit.FunSuite

final class AdjacentImageRollingModelSuite extends FunSuite:
  test("adjacent image configuration parses bounded release inputs") {
    val config = AdjacentImageRollingConfig.parse(Array(
      "--baseline-image", "cascade:1.14.0",
      "--candidate-image", "cascade:1.15.0",
      "--baseline-version", "1.14.0",
      "--candidate-version", "1.15.0",
      "--report", "artifacts/rolling.json",
      "--records-per-phase", "40",
      "--transition-records", "2000",
      "--keep-resources"
    ))
    assertEquals(config.baselineImage, "cascade:1.14.0")
    assertEquals(config.candidateVersion, "1.15.0")
    assertEquals(config.recordsPerPhase, 40)
    assertEquals(config.transitionRecords, 2000)
    assert(config.keepResources)
    assertEquals(config.reportPath, Paths.get("artifacts/rolling.json").toAbsolutePath.normalize())
  }

  test("adjacent image configuration rejects ambiguity and unsafe bounds") {
    val required = Array(
      "--baseline-image", "cascade:1.14.0", "--candidate-image", "cascade:1.15.0",
      "--baseline-version", "1.14.0", "--candidate-version", "1.15.0", "--report", "report.json"
    )
    intercept[IllegalArgumentException](AdjacentImageRollingConfig.parse(required ++ Array("--unknown", "value"))): Unit
    intercept[IllegalArgumentException](AdjacentImageRollingConfig.parse(required ++ Array("--baseline-image", "again"))): Unit
    intercept[IllegalArgumentException](AdjacentImageRollingConfig.parse(required ++ Array("--transition-records", "0"))): Unit
    intercept[IllegalArgumentException](AdjacentImageRollingConfig.parse(required.updated(7, "1.14.0"))): Unit
  }

  test("release image inspection pins identity and security metadata") {
    val expectedId = "sha256:" + "a" * 64
    val runner = new CommandRunner:
      override def run(command: Vector[String], timeout: Duration): CommandResult =
        assert(command.contains("cascade:1.15.0"))
        val labels = s"{\"org.opencontainers.image.created\":\"2026-10-21T12:00:00Z\",\"org.opencontainers.image.revision\":\"${"b" * 40}\",\"org.opencontainers.image.version\":\"1.15.0\"}"
        CommandResult(0, Vector(s"$expectedId\t65532:65532\t$labels\thealth"))
    val image = ReleaseImage.inspect(DockerCli("docker-test", runner), "cascade:1.15.0", "1.15.0")
    assertEquals(image.imageId, expectedId)
    assertEquals(image.revision, "b" * 40)
  }

  test("release image inspection rejects mutable or incomplete identities") {
    def inspect(line: String): Unit =
      val runner = new CommandRunner:
        override def run(command: Vector[String], timeout: Duration): CommandResult = CommandResult(0, Vector(line))
      ReleaseImage.inspect(DockerCli("docker-test", runner), "cascade:1.15.0", "1.15.0"): Unit
    val labels = s"{\"org.opencontainers.image.created\":\"2026-10-21T12:00:00Z\",\"org.opencontainers.image.revision\":\"${"b" * 40}\",\"org.opencontainers.image.version\":\"1.15.0\"}"
    val valid = s"sha256:${"a" * 64}\t65532:65532\t$labels\thealth"
    intercept[IllegalArgumentException](inspect(valid.replace("65532:65532", "0:0"))): Unit
    intercept[IllegalArgumentException](inspect(valid.replace("\"1.15.0\"", "\"1.14.0\""))): Unit
    intercept[IllegalArgumentException](inspect(valid.replace("\thealth", "\tmissing"))): Unit
  }

  test("leader transition probes only surviving bootstrap endpoints") {
    val endpoints = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"
    assertEquals(
      AdjacentImageRollingTraffic.withoutNode(endpoints, 1),
      "127.0.0.1:19093,127.0.0.1:19094"
    )
    assertEquals(
      AdjacentImageRollingTraffic.withoutNode(endpoints, 2),
      "127.0.0.1:19092,127.0.0.1:19094"
    )
    intercept[IllegalArgumentException](AdjacentImageRollingTraffic.withoutNode(endpoints, 4)): Unit
  }

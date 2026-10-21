package cascade.qualification

import java.time.Duration
import munit.FunSuite

final class DockerCliSuite extends FunSuite:
  test("Docker CLI preserves argument boundaries and returns output") {
    var observed = Vector.empty[String]
    val runner = new CommandRunner:
      override def run(command: Vector[String], timeout: Duration): CommandResult =
        observed = command
        assertEquals(timeout, Duration.ofSeconds(7))
        CommandResult(0, Vector("one", "two"))
    val docker = DockerCli("docker-test", runner)
    assertEquals(docker.requireSuccess(Seq("image", "inspect", "name with spaces"), Duration.ofSeconds(7)), Vector("one", "two"))
    assertEquals(observed, Vector("docker-test", "image", "inspect", "name with spaces"))
  }

  test("Docker CLI fails closed on a nonzero exit") {
    val runner = new CommandRunner:
      override def run(command: Vector[String], timeout: Duration): CommandResult = CommandResult(19, Vector("denied"))
    val error = intercept[IllegalStateException](DockerCli("docker-test", runner).requireSuccess(Seq("run", "image")))
    assert(error.getMessage.contains("exit 19"))
    assert(error.getMessage.contains("denied"))
  }

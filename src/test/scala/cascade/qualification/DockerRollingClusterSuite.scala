package cascade.qualification

import java.time.Duration
import munit.FunSuite

final class DockerRollingClusterSuite extends FunSuite:
  private val topology = DockerRollingTopology.forRun("abcdef012345", Vector(19092, 19093, 19094))
  private val image = ReleaseImage("cascade:1.15.0", "sha256:" + "a" * 64, "1.15.0", "b" * 40, "time")

  test("rolling cluster provisions isolated resources and replaces one durable node") {
    val commands = scala.collection.mutable.ArrayBuffer.empty[Vector[String]]
    val runner = new CommandRunner:
      override def run(command: Vector[String], timeout: Duration): CommandResult =
        commands += command
        if command.contains("inspect") then CommandResult(0, Vector("healthy"))
        else CommandResult(0, Vector("ok"))
    val cluster = DockerRollingCluster(DockerCli("docker-test", runner), topology, keepResources = false)
    cluster.provision()
    cluster.startNode(1, image)
    assertEquals(cluster.runningNodeIds, Vector(1))
    cluster.replaceNode(1, image)
    cluster.close()

    assert(commands.exists(_.contains(topology.networkName)))
    assert(commands.count(_.contains(image.imageId)) == 2)
    assert(commands.exists(command => command.contains("stop") && command.contains(topology.containerName(1))))
    assert(commands.exists(command => command.contains("volume") && command.contains("--force")))
    assert(commands.exists(command => command.contains("network") && command.contains("rm")))
  }

  test("rolling cluster never removes retained evidence") {
    val commands = scala.collection.mutable.ArrayBuffer.empty[Vector[String]]
    val runner = new CommandRunner:
      override def run(command: Vector[String], timeout: Duration): CommandResult =
        commands += command
        CommandResult(0, Vector(if command.contains("inspect") then "healthy" else "ok"))
    val cluster = DockerRollingCluster(DockerCli("docker-test", runner), topology, keepResources = true)
    cluster.provision()
    cluster.startNode(2, image)
    cluster.close()
    assert(!commands.exists(command => command.contains("rm") && command.contains("--force")))
    assertEquals(cluster.runningNodeIds, Vector(2))
  }

package cascade.qualification

import munit.FunSuite

final class DockerRollingTopologySuite extends FunSuite:
  private val imageId = "sha256:" + "a" * 64
  private val topology = DockerRollingTopology.forRun("abcdef012345", Vector(19092, 19093, 19094))

  test("rolling topology isolates names, ports and persistent volumes") {
    assertEquals(topology.networkName, "cascade-rolling-abcdef012345-network")
    assertEquals(topology.containerName(2), "cascade-rolling-abcdef012345-node-2")
    assertEquals(topology.volumeName(3), "cascade-rolling-abcdef012345-data-3")
    assertEquals(topology.bootstrapServers, "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094")
  }

  test("rolling broker arguments preserve hardened image and routing contracts") {
    val arguments = topology.runArguments(2, imageId)
    assertEquals(arguments.head, "run")
    assert(arguments.contains("--read-only"))
    assert(arguments.sliding(2).contains(Seq("--cap-drop", "ALL")))
    assert(arguments.sliding(2).contains(Seq("--security-opt", "no-new-privileges:true")))
    assert(arguments.sliding(2).contains(Seq("--publish", "127.0.0.1:19093:9092")))
    assert(arguments.sliding(2).contains(Seq("--mount", "type=volume,source=cascade-rolling-abcdef012345-data-2,target=/var/lib/cascade")))
    assert(arguments.sliding(2).contains(Seq(
      "--cluster-nodes",
      "1@cascade-rolling-abcdef012345-node-1:9092,2@cascade-rolling-abcdef012345-node-2:9092,3@cascade-rolling-abcdef012345-node-3:9092"
    )))
    assert(arguments.sliding(2).contains(Seq(
      "--advertised-cluster-nodes", "1@127.0.0.1:19092,2@127.0.0.1:19093,3@127.0.0.1:19094"
    )))
    assert(arguments.contains(imageId))
  }

  test("rolling topology rejects unsafe or ambiguous resources") {
    intercept[IllegalArgumentException](DockerRollingTopology.forRun("not-safe", Vector(19092, 19093, 19094))): Unit
    intercept[IllegalArgumentException](DockerRollingTopology.forRun("abcdef012345", Vector(19092, 19092, 19094))): Unit
    intercept[IllegalArgumentException](topology.runArguments(4, imageId)): Unit
    intercept[IllegalArgumentException](topology.runArguments(1, "cascade:latest")): Unit
  }

package cascade.qualification

import java.time.Duration
import scala.collection.mutable

private[qualification] final class DockerRollingCluster(
    docker: DockerCli,
    topology: DockerRollingTopology,
    keepResources: Boolean
) extends AutoCloseable:
  private val running = mutable.Set.empty[Int]
  private var provisioned = false

  def provision(): Unit = synchronized {
    require(!provisioned, "rolling cluster is already provisioned")
    docker.requireSuccess(Seq(
      "network", "create", "--driver", "bridge", "--label", "cascade.qualification=adjacent-image", topology.networkName
    ), Duration.ofSeconds(30))
    try
      topology.nodes.foreach { node =>
        docker.requireSuccess(Seq(
          "volume", "create", "--label", "cascade.qualification=adjacent-image", topology.volumeName(node.id)
        ), Duration.ofSeconds(30))
      }
      provisioned = true
    catch
      case error: Throwable =>
        removeResources()
        throw error
  }

  def startNode(nodeId: Int, image: ReleaseImage, waitForReadiness: Boolean = true): Unit = synchronized {
    require(provisioned, "rolling cluster is not provisioned")
    require(!running.contains(nodeId), s"rolling node $nodeId is already running")
    docker.requireSuccess(topology.runArguments(nodeId, image.imageId), Duration.ofMinutes(2))
    running += nodeId
    if waitForReadiness then
      try awaitNodeReady(nodeId)
      catch
        case error: Throwable =>
          val diagnostics = logs(nodeId).mkString(" | ")
          throw IllegalStateException(s"rolling node $nodeId failed readiness: $diagnostics", error)
  }

  def stopNode(nodeId: Int): Unit = synchronized {
    if running.contains(nodeId) then
      docker.requireSuccess(Seq("stop", "--time", "120", topology.containerName(nodeId)), Duration.ofMinutes(3))
      docker.requireSuccess(Seq("rm", topology.containerName(nodeId)), Duration.ofSeconds(30))
      running -= nodeId
  }

  def replaceNode(nodeId: Int, image: ReleaseImage): Unit =
    stopNode(nodeId)
    startNode(nodeId, image)

  def pauseNode(nodeId: Int): Unit = synchronized {
    require(running.contains(nodeId), s"rolling node $nodeId is not running")
    docker.requireSuccess(Seq("pause", topology.containerName(nodeId)), Duration.ofSeconds(30)): Unit
  }

  def unpauseNode(nodeId: Int): Unit = synchronized {
    require(running.contains(nodeId), s"rolling node $nodeId is not running")
    docker.requireSuccess(Seq("unpause", topology.containerName(nodeId)), Duration.ofSeconds(30)): Unit
  }

  def killNode(nodeId: Int): Unit = synchronized {
    if running.contains(nodeId) then
      docker.requireSuccess(Seq("rm", "--force", topology.containerName(nodeId)), Duration.ofSeconds(30))
      running -= nodeId
  }

  def replaceNodeAbruptly(nodeId: Int, image: ReleaseImage): Unit =
    killNode(nodeId)
    startNode(nodeId, image)

  def awaitReady(): Unit =
    running.toVector.sorted.foreach(awaitNodeReady)

  def runningNodeIds: Vector[Int] = synchronized(running.toVector.sorted)

  def logs(nodeId: Int): Vector[String] =
    docker.execute(Seq("logs", topology.containerName(nodeId)), Duration.ofSeconds(30)).output

  def assertNoProtocolErrors(): Unit =
    runningNodeIds.foreach { nodeId =>
      val output = logs(nodeId).mkString("\n")
      require(!output.contains("\"event\":\"protocol_error\""), s"rolling node $nodeId reported a protocol error")
    }

  override def close(): Unit = synchronized {
    if keepResources then
      println(s"ADJACENT_IMAGE_RESOURCES network=${topology.networkName} containers=${runningNodeIds.map(topology.containerName).mkString(",")}")
    else removeResources()
  }

  private def awaitNodeReady(nodeId: Int): Unit =
    val deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos
    var status = "missing"
    while status != "healthy" && System.nanoTime() < deadline do
      val result = docker.execute(Seq(
        "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}",
        topology.containerName(nodeId)
      ), Duration.ofSeconds(10))
      if result.exitCode != 0 then status = "missing"
      else status = result.output.headOption.getOrElse("missing").trim
      if Set("exited", "dead").contains(status) then
        throw IllegalStateException(s"rolling node $nodeId exited before readiness")
      if status != "healthy" then Thread.sleep(250L)
    if status != "healthy" then throw IllegalStateException(s"rolling node $nodeId did not become healthy; status=$status")

  private def removeResources(): Unit =
    topology.nodes.foreach { node =>
      docker.execute(Seq("rm", "--force", topology.containerName(node.id)), Duration.ofSeconds(30)): Unit
      running -= node.id
    }
    topology.nodes.foreach { node =>
      docker.execute(Seq("volume", "rm", "--force", topology.volumeName(node.id)), Duration.ofSeconds(30)): Unit
    }
    docker.execute(Seq("network", "rm", topology.networkName), Duration.ofSeconds(30)): Unit
    provisioned = false

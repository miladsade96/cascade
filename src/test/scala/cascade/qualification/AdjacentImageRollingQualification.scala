package cascade.qualification

import java.net.ServerSocket
import java.time.Duration
import java.time.Instant
import java.util.UUID
import scala.collection.mutable.ArrayBuffer

object AdjacentImageRollingQualification:
  private val Topic = "adjacent-image-rolling"

  def main(arguments: Array[String]): Unit =
    val config = AdjacentImageRollingConfig.parse(arguments)
    val docker = DockerCli(config.dockerExecutable)
    val baseline = ReleaseImage.inspect(docker, config.baselineImage, config.baselineVersion)
    val candidate = ReleaseImage.inspect(docker, config.candidateImage, config.candidateVersion)
    require(baseline.imageId != candidate.imageId, "adjacent releases must resolve to different immutable images")

    val topology = DockerRollingTopology.forRun(
      UUID.randomUUID().toString.replace("-", "").take(12),
      freePorts(3)
    )
    val cluster = DockerRollingCluster(docker, topology, config.keepResources)
    val startedAt = Instant.now()
    val startedNanos = System.nanoTime()
    val phases = ArrayBuffer.empty[String]
    var traffic: AdjacentImageRollingTraffic | Null = null
    var leaderTransitionNode = 0
    var rollbackBeforeUpgrade = false
    var compatibleDowngradeAfterUpgrade = false
    var exactRecovery = false

    try
      cluster.provision()
      (1 to 3).foreach(nodeId => cluster.startNode(nodeId, baseline, waitForReadiness = false))
      cluster.awaitReady()
      phases += s"start-${baseline.version}-cluster"

      traffic = AdjacentImageRollingTraffic.create(topology.bootstrapServers, Topic)
      traffic.nn.produce("baseline", config.recordsPerPhase)
      phases += "baseline-idempotent-traffic"

      val originalLeader = traffic.nn.leaderId
      val rollbackNode = Vector(1, 2, 3).find(_ != originalLeader).get
      cluster.replaceNode(rollbackNode, candidate)
      traffic.nn.produce(s"upgrade-node-$rollbackNode", config.recordsPerPhase)
      phases += s"upgrade-node-$rollbackNode"

      cluster.replaceNode(rollbackNode, baseline)
      traffic.nn.produce(s"rollback-node-$rollbackNode", config.recordsPerPhase)
      rollbackBeforeUpgrade = true
      phases += s"rollback-node-$rollbackNode-before-full-upgrade"

      cluster.replaceNode(rollbackNode, candidate)
      traffic.nn.produce(s"reupgrade-node-$rollbackNode", config.recordsPerPhase)
      phases += s"reupgrade-node-$rollbackNode"

      Vector(1, 2, 3).filter(nodeId => nodeId != rollbackNode && nodeId != originalLeader).foreach { nodeId =>
        cluster.replaceNode(nodeId, candidate)
        traffic.nn.produce(s"upgrade-node-$nodeId", config.recordsPerPhase)
        phases += s"upgrade-node-$nodeId"
      }

      require(traffic.nn.leaderId == originalLeader, "baseline leader changed before the cross-version transition")
      leaderTransitionNode = originalLeader
      cluster.pauseNode(leaderTransitionNode)
      traffic.nn.produceDuring(s"abrupt-leader-$leaderTransitionNode-cross-version", config.transitionRecords) {
        traffic.nn.awaitRetry(Duration.ofSeconds(30))
        cluster.killNode(leaderTransitionNode)
        traffic.nn.awaitLeaderChange(leaderTransitionNode, Duration.ofSeconds(60))
        cluster.startNode(leaderTransitionNode, candidate)
      }
      phases += s"idempotent-retry-while-upgrading-leader-$leaderTransitionNode"
      require(traffic.nn.retryCount > 0L, "leader image transition did not exercise an idempotent producer retry")

      val downgradeNode = Vector(1, 2, 3).find(_ != traffic.nn.leaderId).get
      cluster.replaceNode(downgradeNode, baseline)
      traffic.nn.produce(s"compatible-downgrade-node-$downgradeNode", config.recordsPerPhase)
      compatibleDowngradeAfterUpgrade = true
      phases += s"compatible-downgrade-node-$downgradeNode-after-upgrade"

      cluster.replaceNode(downgradeNode, candidate)
      traffic.nn.produce(s"restore-node-$downgradeNode", config.recordsPerPhase)
      phases += s"restore-node-$downgradeNode-on-${candidate.version}"

      (1 to 3).foreach(cluster.stopNode)
      (1 to 3).foreach(nodeId => cluster.startNode(nodeId, candidate, waitForReadiness = false))
      cluster.awaitReady()
      traffic.nn.produce("candidate-full-restart", config.recordsPerPhase)
      traffic.nn.verifyExact()
      exactRecovery = true
      phases += "exact-record-and-offset-recovery-after-full-restart"
      cluster.assertNoProtocolErrors()

      val report = reportFor(
        "passed", startedAt, startedNanos, baseline, candidate, traffic, leaderTransitionNode,
        rollbackBeforeUpgrade, compatibleDowngradeAfterUpgrade, exactRecovery, phases.toVector, None
      )
      AdjacentImageRollingReport.writeAtomic(config.reportPath, report)
      println(s"ADJACENT_IMAGE_ROLLING_RESULT ${report.json}")
    catch
      case error: Throwable =>
        val report = reportFor(
          "failed", startedAt, startedNanos, baseline, candidate, traffic, leaderTransitionNode,
          rollbackBeforeUpgrade, compatibleDowngradeAfterUpgrade, exactRecovery, phases.toVector,
          Some(Option(error.getMessage).getOrElse(error.getClass.getName))
        )
        AdjacentImageRollingReport.writeAtomic(config.reportPath, report)
        println(s"ADJACENT_IMAGE_ROLLING_RESULT ${report.json}")
        throw error
    finally
      Option(traffic).foreach(_.close())
      cluster.close()

  private def reportFor(
      status: String,
      startedAt: Instant,
      startedNanos: Long,
      baseline: ReleaseImage,
      candidate: ReleaseImage,
      traffic: AdjacentImageRollingTraffic | Null,
      leaderTransitionNode: Int,
      rollbackBeforeUpgrade: Boolean,
      compatibleDowngradeAfterUpgrade: Boolean,
      exactRecovery: Boolean,
      phases: Vector[String],
      error: Option[String]
  ): AdjacentImageRollingReport =
    AdjacentImageRollingReport(
      status,
      startedAt,
      (System.nanoTime() - startedNanos) / 1_000_000_000d,
      baseline,
      candidate,
      Option(traffic).fold(0)(_.recordCount),
      Option(traffic).fold(0L)(_.retryCount),
      leaderTransitionNode,
      rollbackBeforeUpgrade,
      compatibleDowngradeAfterUpgrade,
      exactRecovery,
      phases,
      error
    )

  private def freePorts(count: Int): Vector[Int] =
    val sockets = Vector.fill(count)(ServerSocket(0))
    try sockets.map(_.getLocalPort)
    finally sockets.foreach(_.close())

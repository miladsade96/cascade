package cascade.qualification

private[qualification] final case class DockerRollingNode(id: Int, hostPort: Int):
  require(id >= 1 && id <= 3, "rolling node ID must be between 1 and 3")
  require(hostPort >= 1024 && hostPort <= 65535, "rolling host port is outside the unprivileged range")

private[qualification] final case class DockerRollingTopology(prefix: String, nodes: Vector[DockerRollingNode]):
  require(prefix.matches("cascade-rolling-[0-9a-f]{12}"), "rolling resource prefix is unsafe")
  require(nodes.map(_.id) == Vector(1, 2, 3), "rolling topology requires nodes 1, 2 and 3")
  require(nodes.map(_.hostPort).distinct.size == 3, "rolling host ports must be unique")

  val networkName: String = s"$prefix-network"
  def containerName(nodeId: Int): String = s"$prefix-node-$nodeId"
  def volumeName(nodeId: Int): String = s"$prefix-data-$nodeId"
  def bootstrapServers: String = nodes.map(node => s"127.0.0.1:${node.hostPort}").mkString(",")

  private val internalNodes: String = nodes.map(node => s"${node.id}@${containerName(node.id)}:9092").mkString(",")
  private val advertisedNodes: String = nodes.map(node => s"${node.id}@127.0.0.1:${node.hostPort}").mkString(",")

  def runArguments(nodeId: Int, imageId: String): Vector[String] =
    val node = nodes.find(_.id == nodeId).getOrElse(throw IllegalArgumentException(s"unknown rolling node $nodeId"))
    require(imageId.matches("sha256:[0-9a-f]{64}"), "rolling broker image must use an immutable image ID")
    Vector(
      "run", "--detach", "--init",
      "--name", containerName(nodeId),
      "--hostname", containerName(nodeId),
      "--network", networkName,
      "--network-alias", containerName(nodeId),
      "--read-only",
      "--cap-drop", "ALL",
      "--security-opt", "no-new-privileges:true",
      "--memory", "2g",
      "--tmpfs", "/tmp:size=64m,mode=1777,nosuid,nodev,noexec",
      "--publish", s"127.0.0.1:${node.hostPort}:9092",
      "--mount", s"type=volume,source=${volumeName(nodeId)},target=/var/lib/cascade",
      imageId,
      "--host", "0.0.0.0",
      "--port", "9092",
      "--advertised-host", "127.0.0.1",
      "--advertised-port", node.hostPort.toString,
      "--node-id", nodeId.toString,
      "--data-dir", "/var/lib/cascade",
      "--cluster-nodes", internalNodes,
      "--advertised-cluster-nodes", advertisedNodes,
      "--controller-id", "1",
      "--default-replication-factor", "3",
      "--min-insync-replicas", "2",
      "--peer-timeout-ms", "1000",
      "--controller-heartbeat-ms", "200",
      "--controller-election-timeout-ms", "2000",
      "--flush-policy", "sync",
      "--operations-host", "127.0.0.1",
      "--operations-port", "9404"
    )

private[qualification] object DockerRollingTopology:
  def forRun(runId: String, ports: Vector[Int]): DockerRollingTopology =
    require(runId.matches("[0-9a-f]{12}"), "rolling run ID must contain 12 lowercase hexadecimal characters")
    DockerRollingTopology(s"cascade-rolling-$runId", ports.zipWithIndex.map { case (port, index) =>
      DockerRollingNode(index + 1, port)
    })

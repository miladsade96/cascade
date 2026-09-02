package cascade.coordinator

import cascade.cluster.*
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, NewTopic}

object CoordinatorProbe:
  def activate(bootstrap: String): Unit =
    val properties = Properties()
    properties.setProperty("bootstrap.servers", bootstrap)
    val admin = Admin.create(properties)
    try admin.createTopics(java.util.List.of(NewTopic("coordinator-qualification", 1, 3.toShort))).all().get(20L, TimeUnit.SECONDS): Unit
    finally admin.close(Duration.ofSeconds(2))

  def snapshot(node: ClusterNode): (Long, Int, ClusterMetadata) =
    val peer = PeerClient()
    try
      val response = peer.call(node, InternalApi.MetadataSnapshot, Array.emptyByteArray, 2000)
      val term = response.readLong()
      val leader = response.readInt()
      val metadata = MetadataCodec.decode(response.readByteArray())
      response.ensureFullyRead()
      (term, leader, metadata)
    finally peer.close()

  def controller(nodes: Vector[ClusterNode], excluded: Set[Int] = Set.empty): ClusterNode =
    val deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos
    var found = Option.empty[ClusterNode]
    while found.isEmpty && System.nanoTime() < deadline do
      found = nodes.iterator.filterNot(n => excluded(n.id)).flatMap { node =>
        try
          val (term, leader, metadata) = snapshot(node)
          if node.id != leader || metadata.controllerTerm != term || !metadata.featureLevels.contains(ClusterFeature.CoordinatorDeltas) then None
          else
            // A reported leader may still be acquiring its quorum lease. This impossible expected
            // version probes the active-controller guard without ever publishing a mutation.
            val probe = CoordinatorDelta(term, Vector(CoordinatorShardUpdate(CoordinatorShard.Allocator,
              Long.MaxValue, cascade.protocol.ByteWriter().writeLong(1L).result().toVector)))
            Option.when(commit(node, probe) == cascade.protocol.Errors.CoordinatorLoadInProgress)(node)
        catch case _: Exception => None
      }.nextOption()
      if found.isEmpty then Thread.sleep(25L)
    found.getOrElse(throw IllegalStateException("no delta-capable controller became available"))

  def commit(node: ClusterNode, delta: CoordinatorDelta): Short =
    val peer = PeerClient()
    try
      val response = peer.call(node, InternalApi.CoordinatorDeltaCommit, CoordinatorDeltaCodec.encode(delta), 5000)
      val error = response.readShort()
      response.ensureFullyRead()
      error
    finally peer.close()

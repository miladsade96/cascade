package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
import cascade.delivery.DeliveryShardCodec
import cascade.group.GroupShardCodec
import cascade.protocol.ByteCursor
import scala.util.control.NonFatal

object CoordinatorShardState:
  final case class BatchMerge(metadata: CoordinatorMetadata, accepted: Vector[Boolean]):
    require(accepted.nonEmpty, "coordinator merge batch must not be empty")

  /** Equal shard versions need content identity: failed quorum attempts can reuse a version. */
  def includes(current: CoordinatorMetadata, delta: CoordinatorDelta): Boolean =
    lazy val state = current.shardPayloads
    delta.updates.forall { update =>
      val version = current.shardVersion(update.id)
      version > update.expectedVersion &&
        (version - update.expectedVersion > 1L || state(update.id) == update.payload)
    }

  /** The public coordinator lookup is shared by group and transaction APIs; check both key domains. */
  def readyForKey(installed: CoordinatorMetadata, current: CoordinatorMetadata, key: String): Boolean =
    Vector(CoordinatorShard.group(key), CoordinatorShard.transaction(key)).forall { id =>
      installed.shardVersion(id) >= current.shardVersion(id)
    }

  def readyForShard(installed: CoordinatorMetadata, current: CoordinatorMetadata, shard: Int): Boolean =
    CoordinatorShard.valid(shard) && installed.shardVersion(shard) >= current.shardVersion(shard)

  def payloads(groupState: Vector[Byte], deliveryState: Vector[Byte]): Vector[Vector[Byte]] =
    GroupShardCodec.split(groupState) ++ DeliveryShardCodec.split(deliveryState)

  def changes(
      base: CoordinatorMetadata,
      before: Vector[Vector[Byte]],
      after: Vector[Vector[Byte]],
      controllerTerm: Long
  ): Option[CoordinatorDelta] =
    require(before.size == CoordinatorShard.Count && after.size == CoordinatorShard.Count, "invalid shard snapshot")
    val updates = before.indices.collect {
      case id if before(id) != after(id) => CoordinatorShardUpdate(id, base.shardVersion(id), after(id))
    }.toVector
    Option.when(updates.nonEmpty)(CoordinatorDelta(controllerTerm, updates))

  /** Validate the complete read/write set before building any replacement image. */
  def merge(current: CoordinatorMetadata, delta: CoordinatorDelta, controllerTerm: Long): Either[String, CoordinatorMetadata] =
    mergeOne(current, delta, controllerTerm)

  /** Accept independent deltas in FIFO order while rejecting conflicts without poisoning compatible work. */
  def mergeBatch(current: CoordinatorMetadata, deltas: Vector[CoordinatorDelta], controllerTerm: Long): BatchMerge =
    require(deltas.nonEmpty, "coordinator merge batch must not be empty")
    var candidate = current
    val accepted = deltas.map { delta =>
      mergeOne(candidate, delta, controllerTerm) match
        case Right(next) =>
          candidate = next
          true
        case Left(_) => false
    }
    BatchMerge(candidate, accepted)

  /** Joins independently advancing shard images without allowing an older shard to overwrite a newer one. */
  def mergeMonotonic(left: CoordinatorMetadata, right: CoordinatorMetadata): Either[String, CoordinatorMetadata] =
    try
      val leftPayloads = left.shardPayloads
      val rightPayloads = right.shardPayloads
      val selected = Vector.tabulate(CoordinatorShard.Count) { shard =>
        val comparison = java.lang.Long.compare(left.shardVersion(shard), right.shardVersion(shard))
        if comparison > 0 then (left.shardVersion(shard), leftPayloads(shard))
        else if comparison < 0 then (right.shardVersion(shard), rightPayloads(shard))
        else
          require(leftPayloads(shard) == rightPayloads(shard), s"coordinator shard $shard diverged at equal version")
          (left.shardVersion(shard), leftPayloads(shard))
      }
      val version = math.max(left.version, right.version)
      val payloads = selected.map(_._2)
      Right(CoordinatorMetadata(
        version,
        math.max(left.ownerTerm, right.ownerTerm),
        GroupShardCodec.merge(payloads.take(CoordinatorShard.Buckets), version),
        DeliveryShardCodec.merge(payloads.drop(CoordinatorShard.Buckets), version),
        selected.map(_._1)
      ))
    catch case NonFatal(error) => Left(error.getMessage)

  /** Installs one independently forced journal checkpoint without regressing any other shard. */
  def installCheckpoint(current: CoordinatorMetadata, checkpoint: CoordinatorShardCheckpoint): Either[String, CoordinatorMetadata] =
    try
      val existingVersion = current.shardVersion(checkpoint.shard)
      val payloads = current.shardPayloads
      if checkpoint.shardVersion < existingVersion then Right(current)
      else if checkpoint.shardVersion == existingVersion then
        if payloads(checkpoint.shard) == checkpoint.payload then Right(current)
        else Left(s"coordinator shard ${checkpoint.shard} checkpoint diverged at equal version")
      else
        val nextPayloads = payloads.updated(checkpoint.shard, checkpoint.payload)
        val versions = Vector.tabulate(CoordinatorShard.Count)(current.shardVersion)
          .updated(checkpoint.shard, checkpoint.shardVersion)
        val imageVersion = math.max(current.version, checkpoint.imageVersion)
        Right(CoordinatorMetadata(
          imageVersion,
          math.max(current.ownerTerm, checkpoint.ownerTerm),
          GroupShardCodec.merge(nextPayloads.take(CoordinatorShard.Buckets), imageVersion),
          DeliveryShardCodec.merge(nextPayloads.drop(CoordinatorShard.Buckets), imageVersion),
          versions
        ))
    catch case NonFatal(error) => Left(s"invalid coordinator shard checkpoint: ${error.getMessage}")

  private def mergeOne(current: CoordinatorMetadata, delta: CoordinatorDelta, controllerTerm: Long): Either[String, CoordinatorMetadata] =
    if delta.controllerTerm != controllerTerm then Left("stale controller term")
    else if delta.updates.exists(update => current.shardVersion(update.id) != update.expectedVersion) then Left("stale coordinator shard")
    else
      try
        val before = current.shardPayloads
        val after = delta.updates.foldLeft(before)((state, update) => state.updated(update.id, update.payload))
        val oldAllocation = ByteCursor(before(CoordinatorShard.Allocator).toArray).readLong()
        val newAllocation = ByteCursor(after(CoordinatorShard.Allocator).toArray).readLong()
        require(newAllocation >= oldAllocation, "producer allocation cannot move backward")
        val version = Math.addExact(current.version, 1L)
        val initialVersions = Vector.tabulate(CoordinatorShard.Count)(current.shardVersion)
        val versions = delta.updates.foldLeft(initialVersions) { (state, update) =>
          state.updated(update.id, Math.addExact(update.expectedVersion, 1L))
        }
        Right(CoordinatorMetadata(
          version, controllerTerm,
          GroupShardCodec.merge(after.take(CoordinatorShard.Buckets), version),
          DeliveryShardCodec.merge(after.drop(CoordinatorShard.Buckets), version),
          versions
        ))
      catch case NonFatal(error) => Left(s"invalid coordinator delta: ${error.getMessage}")

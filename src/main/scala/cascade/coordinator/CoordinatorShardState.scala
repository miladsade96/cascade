package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
import cascade.delivery.DeliveryShardCodec
import cascade.group.GroupShardCodec
import cascade.protocol.ByteCursor
import scala.util.control.NonFatal

object CoordinatorShardState:
  /** The public coordinator lookup is shared by group and transaction APIs; check both key domains. */
  def readyForKey(installed: CoordinatorMetadata, current: CoordinatorMetadata, key: String): Boolean =
    Vector(CoordinatorShard.group(key), CoordinatorShard.transaction(key)).forall { id =>
      installed.shardVersion(id) >= current.shardVersion(id)
    }

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
    if delta.controllerTerm != controllerTerm then Left("stale controller term")
    else if delta.updates.exists(update => current.shardVersion(update.id) != update.expectedVersion) then Left("stale coordinator shard")
    else
      try
        val before = payloads(current.groupState, current.deliveryState)
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

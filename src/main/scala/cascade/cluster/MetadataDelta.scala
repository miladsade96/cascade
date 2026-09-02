package cascade.cluster

import cascade.coordinator.{CoordinatorDelta, CoordinatorShardState}
import scala.util.control.NonFatal

/** An atomic coordinator-only transition in the existing ordered metadata quorum. */
final case class MetadataDelta(baseVersion: Long, baseCoordinatorVersion: Long, change: CoordinatorDelta):
  require(baseVersion >= 0L && baseCoordinatorVersion >= 0L, "negative metadata delta base")

  def applyTo(base: ClusterMetadata): Either[String, ClusterMetadata] =
    if !MetadataDelta.enabled(base) then Left("incremental coordinator feature is not active")
    else if base.version != baseVersion || base.coordinator.version != baseCoordinatorVersion then
      Left("metadata delta base version mismatch")
    else if base.controllerTerm != change.controllerTerm then Left("metadata delta base term mismatch")
    else
      CoordinatorShardState.merge(base.coordinator, change, base.controllerTerm).flatMap { coordinator =>
        try Right(base.copy(version = Math.addExact(baseVersion, 1L), coordinator = coordinator))
        catch case NonFatal(error) => Left(error.getMessage)
      }

object MetadataDelta:
  def enabled(metadata: ClusterMetadata): Boolean =
    metadata.featureLevels.getOrElse(ClusterFeature.IncrementalCoordinator, 0.toShort) >= 1 &&
      metadata.featureLevels.getOrElse(ClusterFeature.CoordinatorDeltas, 0.toShort) >= 1

  /** Structural changes and non-consecutive recovery always retain the full-snapshot path. */
  def between(base: ClusterMetadata, next: ClusterMetadata): Option[MetadataDelta] =
    if !enabled(base) || next.copy(version = base.version, coordinator = base.coordinator) != base then None
    else
      try
        val before = CoordinatorShardState.payloads(base.coordinator.groupState, base.coordinator.deliveryState)
        val after = CoordinatorShardState.payloads(next.coordinator.groupState, next.coordinator.deliveryState)
        CoordinatorShardState.changes(base.coordinator, before, after, next.controllerTerm)
          .map(MetadataDelta(base.version, base.coordinator.version, _))
          .filter(_.applyTo(base).contains(next))
      catch case NonFatal(_) => None

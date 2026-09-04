package cascade.coordinator

import cascade.group.{GroupImage, GroupSnapshotCache}
import cascade.delivery.{DeliveryImage, DeliverySnapshotCache}

/** Called under the combined service lock so group offsets and transaction outcomes stay atomic. */
private[cascade] final class CoordinatorSnapshotCache:
  private val groups = GroupSnapshotCache()
  private val delivery = DeliverySnapshotCache()

  def capture(groupImage: GroupImage, deliveryImage: DeliveryImage): CoordinatorSnapshot =
    val groupState = groups.capture(groupImage)
    val deliveryState = delivery.capture(deliveryImage)
    CoordinatorSnapshot(
      groupState.payloads ++ deliveryState.payloads,
      GroupSnapshotCache.fullImageBytes(groupState.payloads) + DeliverySnapshotCache.fullImageBytes(deliveryState.payloads),
      groupState.encoded + deliveryState.encoded,
      groupState.reused + deliveryState.reused,
      groupState.encodedBytes + deliveryState.encodedBytes
    )

private[cascade] final case class CoordinatorSnapshot(
    payloads: Vector[Vector[Byte]], fullImageBytes: Long, encoded: Int, reused: Int, encodedBytes: Long
)

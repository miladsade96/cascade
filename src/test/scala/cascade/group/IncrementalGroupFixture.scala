package cascade.group

import cascade.coordinator.CoordinatorShard

object IncrementalGroupFixture:
  def offsetShard(state: Vector[Byte], group: String, offset: Long): Vector[Byte] =
    val image = if state.isEmpty then GroupImage.Empty else GroupCodec.decode(state.toArray)
    val offsets = image.offsets.filterNot(_.key.groupId == group) :+
      OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, System.currentTimeMillis()))
    GroupShardCodec.split(GroupCodec.encode(image.copy(version = 0L, offsets = offsets)).toVector)(CoordinatorShard.group(group))

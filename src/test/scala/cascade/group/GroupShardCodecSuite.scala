package cascade.group

import cascade.coordinator.CoordinatorShard
import munit.FunSuite

final class GroupShardCodecSuite extends FunSuite:
  private val offsets = (0 until 1000).map { i =>
    OffsetCommitValue(GroupOffsetKey(s"group-$i", "events", 0), CommittedOffset(i.toLong, -1, None, 123L))
  }.toVector.sortBy(_.key.groupId)
  private val image = GroupImage(9L, Vector.empty, offsets)

  test("group shard split and merge preserve exact high-cardinality offsets") {
    val shards = GroupShardCodec.split(GroupCodec.encode(image).toVector)
    assertEquals(shards.size, 64)
    assertEquals(GroupCodec.decode(GroupShardCodec.merge(shards, 10L).toArray), image.copy(version = 10L))
    assertEquals(shards, GroupShardCodec.split(GroupCodec.encode(image.copy(version = 99L)).toVector))
  }

  test("updates carry only one group shard and empty replacement deletes its offsets") {
    val original = GroupShardCodec.split(GroupCodec.encode(image).toVector)
    val changed = image.copy(offsets = offsets.updated(0, offsets.head.copy(value = offsets.head.value.copy(offset = 9999L))))
    val next = GroupShardCodec.split(GroupCodec.encode(changed).toVector)
    assertEquals(original.zip(next).count((a, b) => a != b), 1)
    val id = CoordinatorShard.group(offsets.head.key.groupId)
    val empty = GroupShardCodec.split(Vector.empty)
    val recovered = GroupCodec.decode(GroupShardCodec.merge(original.updated(id, empty(id)), 11L).toArray)
    assert(recovered.offsets.forall(value => CoordinatorShard.group(value.key.groupId) != id))
  }

  test("a payload cannot put offsets in another shard") {
    val original = GroupShardCodec.split(GroupCodec.encode(image).toVector)
    intercept[IllegalArgumentException](GroupShardCodec.merge(original.updated(0, original(1)), 10L))
  }

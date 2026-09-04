package cascade.group

import munit.FunSuite

final class GroupSnapshotCacheSuite extends FunSuite:
  private def offset(group: String, value: Long) =
    OffsetCommitValue(GroupOffsetKey(group, "événements", 0), CommittedOffset(value, 2, Some("پیام"), 42L))

  test("a single offset change re-encodes one of 64 buckets and version-only changes encode none") {
    val cache = GroupSnapshotCache()
    val original = GroupImage(7L, Vector.empty, (0 until 1000).map(i => offset(s"group-$i", i.toLong)).toVector)
    val first = cache.capture(original)
    assertEquals(first.encoded, 64)
    assertEquals(cache.capture(original.copy(version = 8L)).reused, 64)
    val changed = original.copy(offsets = original.offsets.updated(12, offset("group-12", 99L)))
    val after = cache.capture(changed)
    assertEquals((after.encoded, after.reused), (1, 63))
    assertEquals(after.payloads, GroupShardCodec.split(changed))
    assertEquals(GroupSnapshotCache.fullImageBytes(after.payloads), GroupCodec.encode(changed).length.toLong)
  }

  test("format-one and format-two size accounting exactly matches full encoding") {
    val cache = GroupSnapshotCache()
    val consumer = StoredConsumerGroup("modern", 1, Vector.empty)
    Vector(GroupImage.Empty, GroupImage(3L, Vector.empty, Vector(offset("g", 2))),
      GroupImage(4L, Vector.empty, Vector(offset("g", 3)), Vector(consumer))).foreach { image =>
      assertEquals(GroupSnapshotCache.fullImageBytes(cache.capture(image).payloads), GroupCodec.encode(image).length.toLong)
    }
  }

package cascade.group

import java.nio.file.Files
import munit.FunSuite

final class OffsetViewCacheSuite extends FunSuite:
  private def value(group: String, offset: Long, at: Long = 42L) =
    OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, at))

  private def withStore(body: OffsetStore => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-offset-view-")
    val store = OffsetStore(directory.resolve("offsets.log"))
    try body(store)
    finally
      store.close()
      Files.deleteIfExists(directory.resolve("offsets.log")): Unit
      Files.deleteIfExists(directory): Unit

  test("ordered offset views are immutable and reused until a real mutation") {
    withStore { store =>
      store.commit(Vector(value("b", 2L), value("a", 1L)), durable = false)
      val before = store.entries
      assert(before eq store.entries)
      store.install(before)
      assert(before eq store.entries)
      store.commit(Vector(value("a", 0L)), durable = false)
      assertEquals(before.head.value.offset, 1L)
      assertEquals(store.entries.head.value.offset, 0L)
      store.install(before)
      assertEquals(store.entries, before)
    }
  }

  test("expiry invalidates cached views only for eligible keys and never retains deleted offsets") {
    withStore { store =>
      store.commit(Vector(value("a", 1L), value("b", 2L)), durable = false)
      val before = store.entries
      assertEquals(store.expireBefore(42L, durable = false), Vector.empty)
      assert(before eq store.entries)
      assertEquals(store.expireBefore(43L, durable = false, eligible = _.groupId == "a"), Vector(value("a", 1L).key))
      assertEquals(store.entries, Vector(value("b", 2L)))
      store.install(Vector.empty)
      assertEquals(store.entries, Vector.empty)
    }
  }

  test("group-indexed fetch remains exact through high-cardinality rewinds replacements and deletion") {
    withStore { store =>
      val values = (0 until 1000).map(i => value(s"workers-$i", i.toLong)).toVector
      store.commit(values, durable = false)
      store.commit(Vector(value("workers-123", 0L)), durable = false)
      assertEquals(store.all("workers-123"), Vector(value("workers-123", 0L).key -> value("workers-123", 0L).value))
      assertEquals(store.all("missing"), Vector.empty)
      store.install(Vector(value("replacement", 42L)))
      assertEquals(store.all("workers-123"), Vector.empty)
      assertEquals(store.all("replacement").map(_._2.offset), Vector(42L))
      store.expireBefore(43L, durable = false)
      assertEquals(store.all("replacement"), Vector.empty)
    }
  }

  test("the group index is rebuilt after durable compaction and journal recovery") {
    val directory = Files.createTempDirectory("cascade-offset-index-recovery-")
    val path = directory.resolve("offsets.log")
    val original = OffsetStore(path)
    try
      original.commit(Vector(value("a", 1L), value("b", 2L), value("a", 3L)))
      original.compact()
    finally original.close()
    val recovered = OffsetStore(path)
    try
      assertEquals(recovered.all("a"), Vector(value("a", 3L).key -> value("a", 3L).value))
      assertEquals(recovered.all("b"), Vector(value("b", 2L).key -> value("b", 2L).value))
      assertEquals(recovered.entries, Vector(value("a", 3L), value("b", 2L)))
    finally
      recovered.close()
      Files.deleteIfExists(path): Unit
      Files.deleteIfExists(directory): Unit
  }

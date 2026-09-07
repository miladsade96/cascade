package cascade.group

import munit.FunSuite

final class OffsetReadViewSuite extends FunSuite:
  test("builds deterministic key and group indexes") {
    val second = value("workers", "events", 1, 20L)
    val first = value("workers", "events", 0, 10L)
    val other = value("billing", "charges", 2, 30L)

    val view = OffsetReadView.from(Vector(second, other, first))

    assertEquals(view.entries, Vector(other, first, second))
    assertEquals(view.get(first.key), Some(first.value))
    assertEquals(view.all("workers"), Vector(first.key -> first.value, second.key -> second.value))
    assertEquals(view.all("missing"), Vector.empty)
  }

  test("the view is detached from its source collection") {
    val source = scala.collection.mutable.ArrayBuffer(value("workers", "events", 0, 10L))
    val view = OffsetReadView.from(source)
    source += value("workers", "events", 1, 20L)

    assertEquals(view.entries.size, 1)
    assertEquals(view.get(source.last.key), None)
  }

  test("updates and removes only touched groups while retaining old views") {
    val first = value("workers", "events", 0, 10L)
    val second = value("workers", "events", 1, 20L)
    val untouched = value("billing", "charges", 0, 30L)
    val before = OffsetReadView.from(Vector(first, second, untouched))

    val replacement = value("workers", "events", 0, 40L)
    val after = before.updated(Vector(replacement), Vector(second.key))

    assertEquals(after.all("workers"), Vector(replacement.key -> replacement.value))
    assertEquals(after.all("billing"), before.all("billing"))
    assertEquals(after.get(second.key), None)
    assertEquals(before.get(first.key), Some(first.value))
    assertEquals(before.get(second.key), Some(second.value))
  }

  test("reuses untouched group indexes and the whole view for no-op updates") {
    val workers = value("workers", "events", 0, 10L)
    val billing = value("billing", "charges", 0, 30L)
    val before = OffsetReadView.from(Vector(workers, billing))

    val after = before.updated(Vector(value("workers", "events", 0, 20L)))
    assert(after.byGroup("billing").eq(before.byGroup("billing")))

    val unchanged = after.updated(Vector.empty, Vector.empty)
    assert(unchanged.eq(after))
  }

  test("an upsert wins over a simultaneous removal of the same key") {
    val first = value("workers", "events", 0, 10L)
    val replacement = value("workers", "events", 0, 20L)

    val view = OffsetReadView.from(Vector(first)).updated(Vector(replacement), Vector(first.key))

    assertEquals(view.get(first.key), Some(replacement.value))
  }

  private def value(group: String, topic: String, partition: Int, offset: Long): OffsetCommitValue =
    OffsetCommitValue(GroupOffsetKey(group, topic, partition), CommittedOffset(offset, -1, None, offset))

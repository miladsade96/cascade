package cascade.group

import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class OffsetStoreSuite extends FunSuite:
  test("committed offsets survive restart and a partial journal tail") {
    val directory = Files.createTempDirectory("cascade-offset-store-test")
    val path = directory.resolve("offsets.log")
    val first = OffsetCommitValue(
      GroupOffsetKey("workers", "events", 0),
      CommittedOffset(42L, 3, Some("checkpoint"), 1000L)
    )
    val second = OffsetCommitValue(
      GroupOffsetKey("workers", "events", 1),
      CommittedOffset(99L, -1, None, 2000L)
    )
    try
      val store = OffsetStore(path)
      try store.commit(Vector(first, second))
      finally store.close()
      val completeSize = Files.size(path)
      Files.write(path, Array[Byte](0, 0, 0), StandardOpenOption.APPEND)

      val recovered = OffsetStore(path)
      try
        assertEquals(recovered.get(first.key), Some(first.value))
        assertEquals(recovered.get(second.key), Some(second.value))
        assertEquals(recovered.all("workers").map(_._1), Vector(first.key, second.key))
        assertEquals(Files.size(path), completeSize)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("checksum-corrupt journal tail is discarded without losing earlier commits") {
    val directory = Files.createTempDirectory("cascade-offset-checksum-test")
    val path = directory.resolve("offsets.log")
    val first = OffsetCommitValue(
      GroupOffsetKey("workers", "events", 0),
      CommittedOffset(10L, -1, None, 1000L)
    )
    val second = OffsetCommitValue(
      GroupOffsetKey("workers", "events", 1),
      CommittedOffset(20L, -1, None, 2000L)
    )
    try
      val store = OffsetStore(path)
      val firstFrameSize =
        try
          store.commit(Vector(first))
          val size = Files.size(path)
          store.commit(Vector(second))
          size
        finally store.close()

      val journal = Files.readAllBytes(path)
      journal(journal.length - 1) = (journal.last ^ 0xff).toByte
      Files.write(path, journal)

      val recovered = OffsetStore(path)
      try
        assertEquals(recovered.get(first.key), Some(first.value))
        assertEquals(recovered.get(second.key), None)
        assertEquals(Files.size(path), firstFrameSize)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("quorum snapshots can replace the in-memory offset view without changing the local journal") {
    val directory = Files.createTempDirectory("cascade-offset-snapshot-test")
    val path = directory.resolve("offsets.log")
    val durable = OffsetCommitValue(
      GroupOffsetKey("workers", "events", 0),
      CommittedOffset(10L, 1, None, 1000L)
    )
    val replicated = OffsetCommitValue(
      GroupOffsetKey("workers", "events", 1),
      CommittedOffset(20L, 2, Some("replicated"), 2000L)
    )
    try
      val store = OffsetStore(path)
      try
        store.commit(Vector(durable))
        val journalSize = Files.size(path)
        store.commit(Vector(replicated), durable = false)
        assertEquals(store.entries, Vector(durable, replicated))
        assertEquals(Files.size(path), journalSize)

        store.install(Vector(replicated))
        assertEquals(store.entries, Vector(replicated))
        assertEquals(Files.size(path), journalSize)
      finally store.close()

      val recovered = OffsetStore(path)
      try assertEquals(recovered.entries, Vector(durable))
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("staged offsets remain invisible until their acknowledged view is published") {
    val directory = Files.createTempDirectory("cascade-offset-read-view-test")
    val path = directory.resolve("offsets.log")
    val key = GroupOffsetKey("workers", "events", 0)
    val first = OffsetCommitValue(key, CommittedOffset(10L, -1, None, 1000L))
    val second = OffsetCommitValue(key, CommittedOffset(20L, -1, None, 2000L))
    try
      val store = OffsetStore(path)
      try
        store.commit(Vector(first))
        val before = store.acknowledgedView
        store.commit(Vector(second), durable = false, publish = false)

        assertEquals(store.entries, Vector(second))
        assertEquals(store.get(key), Some(first.value))
        assertEquals(before.get(key), Some(first.value))

        store.publishAcknowledged()
        assertEquals(store.get(key), Some(second.value))
        assertEquals(before.get(key), Some(first.value))
      finally store.close()
    finally deleteTree(directory)
  }

  test("compaction atomically retains only the latest offset for every key") {
    val directory = Files.createTempDirectory("cascade-offset-compaction-test")
    val path = directory.resolve("offsets.log")
    val key = GroupOffsetKey("workers", "events", 0)
    try
      val store = OffsetStore(path)
      val before =
        try
          (1L to 20L).foreach { offset =>
            store.commit(Vector(OffsetCommitValue(key, CommittedOffset(offset, -1, None, offset * 1000L))))
          }
          val size = store.journalSize
          store.compact()
          assert(store.journalSize < size)
          assertEquals(store.get(key).map(_.offset), Some(20L))
          size
        finally store.close()

      val recovered = OffsetStore(path)
      try
        assertEquals(recovered.get(key).map(_.offset), Some(20L))
        assert(recovered.journalSize < before)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("offset expiration removes old commits durably and preserves recent commits") {
    val directory = Files.createTempDirectory("cascade-offset-expiration-test")
    val path = directory.resolve("offsets.log")
    val old = OffsetCommitValue(GroupOffsetKey("old", "events", 0), CommittedOffset(10L, -1, None, 1000L))
    val recent = OffsetCommitValue(GroupOffsetKey("recent", "events", 0), CommittedOffset(20L, -1, None, 3000L))
    try
      val store = OffsetStore(path)
      try
        store.commit(Vector(old, recent))
        assertEquals(store.expireBefore(2000L), Vector(old.key))
        assertEquals(store.get(old.key), None)
        assertEquals(store.get(recent.key), Some(recent.value))
      finally store.close()

      val recovered = OffsetStore(path)
      try
        assertEquals(recovered.get(old.key), None)
        assertEquals(recovered.get(recent.key), Some(recent.value))
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("staged expiration preserves the acknowledged view until publication") {
    val directory = Files.createTempDirectory("cascade-offset-expiration-view-test")
    val path = directory.resolve("offsets.log")
    val old = OffsetCommitValue(GroupOffsetKey("workers", "events", 0), CommittedOffset(10L, -1, None, 1000L))
    val recent = OffsetCommitValue(GroupOffsetKey("workers", "events", 1), CommittedOffset(20L, -1, None, 3000L))
    try
      val store = OffsetStore(path)
      try
        store.commit(Vector(old, recent), durable = false)
        assertEquals(store.expireBefore(2000L, durable = false, publish = false), Vector(old.key))
        assertEquals(store.get(old.key), Some(old.value))
        assertEquals(store.get(recent.key), Some(recent.value))

        store.publishAcknowledged()
        assertEquals(store.get(old.key), None)
        assertEquals(store.get(recent.key), Some(recent.value))
      finally store.close()
    finally deleteTree(directory)
  }

  test("group removal stays invisible until its acknowledged view is published") {
    val directory = Files.createTempDirectory("cascade-offset-group-removal-view-test")
    val path = directory.resolve("offsets.log")
    val first = OffsetCommitValue(GroupOffsetKey("workers", "events", 0), CommittedOffset(10L, -1, None, 1000L))
    val second = OffsetCommitValue(GroupOffsetKey("workers", "events", 1), CommittedOffset(20L, -1, None, 1000L))
    try
      val store = OffsetStore(path)
      try
        store.commit(Vector(first, second), durable = false)
        assertEquals(store.removeGroup("workers", durable = false, publish = false), Vector(first.key, second.key))
        assertEquals(store.entries, Vector.empty)
        assertEquals(store.all("workers"), Vector(first.key -> first.value, second.key -> second.value))

        store.publishAcknowledged()
        assertEquals(store.all("workers"), Vector.empty)
        assertEquals(store.removeGroup("missing", durable = false), Vector.empty)
      finally store.close()
    finally deleteTree(directory)
  }

  test("durable group removal survives offset-store restart") {
    val directory = Files.createTempDirectory("cascade-offset-group-removal-restart-test")
    val path = directory.resolve("offsets.log")
    val removed = OffsetCommitValue(GroupOffsetKey("workers", "events", 0), CommittedOffset(10L, -1, None, 1000L))
    val retained = OffsetCommitValue(GroupOffsetKey("billing", "events", 0), CommittedOffset(20L, -1, None, 1000L))
    try
      val store = OffsetStore(path)
      try
        store.commit(Vector(removed, retained))
        assertEquals(store.removeGroup("workers"), Vector(removed.key))
      finally store.close()

      val recovered = OffsetStore(path)
      try
        assertEquals(recovered.all("workers"), Vector.empty)
        assertEquals(recovered.all("billing"), Vector(retained.key -> retained.value))
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

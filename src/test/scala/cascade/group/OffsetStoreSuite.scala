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

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

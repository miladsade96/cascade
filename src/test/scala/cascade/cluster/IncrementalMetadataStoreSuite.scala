package cascade.cluster

import cascade.protocol.ByteCursor
import java.nio.file.{Files, Path}
import munit.FunSuite

final class IncrementalMetadataStoreSuite extends FunSuite:
  import MetadataDeltaFixture.*

  private def withJournal(test: Path => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-incremental-journal")
    val path = directory.resolve("metadata.log")
    try test(path)
    finally
      Files.deleteIfExists(path): Unit
      Files.deleteIfExists(directory): Unit

  test("journal stores one delta frame per atomic update and replays exact state") {
    withJournal { path =>
      val (_, first) = update(base)
      val (delta, second) = update(first, "another-group", 20L)
      val store = MetadataStore(path)
      try
        store.commit(base)
        store.commit(first)
        val before = store.journalSize
        store.commit(second)
        assertEquals(store.journalSize - before, MetadataDeltaCodec.encode(delta).length.toLong + 8L)
        assertEquals(store.metadata, second)
        assertEquals(store.snapshot.fullRecords, 1L)
        assertEquals(store.snapshot.deltaRecords, 2L)
        assertEquals(store.snapshot.journalBytes, store.journalSize)
        assertEquals(store.snapshot.fullBytes + store.snapshot.deltaBytes, store.journalSize)
      finally store.close()
      val recovered = MetadataStore(path)
      try
        assertEquals(recovered.metadata, second)
        assertEquals(recovered.snapshot.deltaRecords, 0L)
        assertEquals(recovered.snapshot.journalBytes, Files.size(path))
      finally recovered.close()
    }
  }

  test("legacy writes remain full snapshots and structural mutations reset the replay base") {
    withJournal { path =>
      val legacy = base.copy(featureLevels = Map.empty)
      val store = MetadataStore(path)
      val (_, next) = update(base)
      val structural = next.copy(version = next.version + 1L, unavailableBrokerIds = Set(3))
      val (_, finalImage) = update(structural, "workers", 50L)
      try
        store.commit(legacy)
        store.commit(base.copy(version = legacy.version + 1L))
        store.commit(structural)
        store.commit(finalImage)
        val bytes = Files.readAllBytes(path)
        val cursor = ByteCursor(bytes)
        val first = cursor.readBytes(cursor.readInt())
        assert(!MetadataDeltaCodec.isDelta(first))
      finally store.close()
      val recovered = MetadataStore(path)
      try assertEquals(recovered.metadata, finalImage)
      finally recovered.close()
    }
  }

package cascade.cluster

import cascade.coordinator.{CoordinatorDelta, CoordinatorShard, CoordinatorShardUpdate}
import cascade.group.IncrementalGroupFixture
import cascade.protocol.ProtocolException
import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class ShardMetadataStoreSuite extends FunSuite:
  import ShardStorageFixture.*

  test("activated journals contain references and restart from independent shard payloads") {
    withDirectory { root =>
      val path = root.resolve("metadata.log")
      val (_, next) = MetadataDeltaFixture.update(active)
      val store = MetadataStore(path)
      try
        store.commit(active)
        val size = store.journalSize
        store.commit(next)
        assert(store.journalSize - size < 170L)
        assertEquals(store.snapshot.deltaRecords, 1L)
        assertEquals(store.objectSnapshot.writtenObjects, 3L)
        assert(store.objectSnapshot.writtenBytes > 0L)
      finally store.close()
      val recovered = MetadataStore(path)
      try assertEquals(recovered.metadata, next)
      finally recovered.close()
    }
  }

  test("missing and corrupt referenced objects reject startup without trimming the journal") {
    for corrupt <- Vector(false, true) do withDirectory { root =>
      val path = root.resolve("metadata.log")
      val (_, next) = MetadataDeltaFixture.update(active)
      val store = MetadataStore(path)
      try
        store.commit(active)
        store.commit(next)
      finally store.close()
      val objectDirectory = ShardObjectStore.pathFor(path)
      val paths = Files.list(objectDirectory)
      val target = try paths.iterator().asScala.find(_.getFileName.toString.startsWith(s"${CoordinatorShard.group("workers")}-")).get
        finally paths.close()
      val before = Files.readAllBytes(path).toVector
      if corrupt then
        val bytes = Files.readAllBytes(target)
        bytes(bytes.length - 1) = (bytes.last ^ 1).toByte
        Files.write(target, bytes): Unit
      else Files.delete(target)
      intercept[ProtocolException](MetadataStore(path))
      assertEquals(Files.readAllBytes(path).toVector, before)
    }
  }

  test("prepared but unpublished objects are ignored after restart") {
    withDirectory { root =>
      val path = root.resolve("metadata.log")
      val store = MetadataStore(path)
      try store.commit(active)
      finally store.close()
      ShardObjectStore(ShardObjectStore.pathFor(path)).put(0, Array[Byte](99))
      val recovered = MetadataStore(path)
      try assertEquals(recovered.metadata, active)
      finally recovered.close()
    }
  }

  test("torn publication markers never expose prepared shard changes") {
    for missing <- Vector(1, 4, 17, 60) do withDirectory { root =>
      val path = root.resolve("metadata.log")
      val (_, next) = MetadataDeltaFixture.update(active)
      val store = MetadataStore(path)
      var baseSize = 0L
      try
        store.commit(active)
        baseSize = store.journalSize
        store.commit(next)
      finally store.close()
      val channel = java.nio.channels.FileChannel.open(path, StandardOpenOption.WRITE)
      try channel.truncate(channel.size() - missing)
      finally channel.close()
      val recovered = MetadataStore(path)
      try
        assertEquals(recovered.metadata, active)
        assertEquals(Files.size(path), baseSize)
        recovered.commit(next)
        assertEquals(recovered.metadata, next)
      finally recovered.close()
    }
  }

  test("one marker publishes multiple shards atomically and rejects a missing member") {
    withDirectory { root =>
      val first = "workers"
      val second = Iterator.from(0).map(i => s"other-$i").find(CoordinatorShard.group(_) != CoordinatorShard.group(first)).get
      val change = CoordinatorDelta(active.controllerTerm, Vector(first, second).map { group =>
        CoordinatorShardUpdate(CoordinatorShard.group(group), 0L, IncrementalGroupFixture.offsetShard(Vector.empty, group, 42L))
      })
      val delta = MetadataDelta(active.version, 0L, active.fingerprint, change)
      val next = delta.applyTo(active).toOption.get
      val objects = ShardObjectStore(root)
      val record = ShardMetadataRecord.prepare(active, next, objects)
      assertEquals(record.references.size, 2)
      assertEquals(ShardMetadataRecord.replay(record.bytes, active, objects).metadata, next)
      Files.delete(root.resolve(record.references.head.fileName))
      intercept[ProtocolException](ShardMetadataRecord.replay(record.bytes, active, objects)): Unit
    }
  }

  test("checkpoints bound marker replay and reclaim only when directory publication is forced") {
    withDirectory { root =>
      val path = root.resolve("metadata.log")
      val store = MetadataStore(path, 1024L)
      var next = active
      try
        store.commit(next)
        (1 to 50).foreach { offset =>
          next = MetadataDeltaFixture.update(next, "workers", offset.toLong)._2
          store.commit(next)
        }
        assert(store.snapshot.checkpointBytes > 0L)
        assert(store.journalSize < 2200L)
        if ShardObjectStore.forceDirectory(root) then assert(store.objectSnapshot.reclaimedObjects > 0L)
        else assertEquals(store.objectSnapshot.reclaimedObjects, 0L)
      finally store.close()
      val recovered = MetadataStore(path, 1024L)
      try assertEquals(recovered.metadata, next)
      finally recovered.close()
    }
  }

  test("an inactive feature retains the inline journal without an object directory") {
    withDirectory { root =>
      val path = root.resolve("metadata.log")
      val store = MetadataStore(path)
      try store.commit(MetadataDeltaFixture.base)
      finally store.close()
      assert(!Files.exists(ShardObjectStore.pathFor(path)))
    }
  }

package cascade.cluster

import cascade.protocol.ProtocolException
import java.nio.file.{Files, Path}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

object ShardStorageFixture:
  val active = MetadataDeltaFixture.base.copy(featureLevels = PeerCapabilities.Current.featureLevels)
  def withDirectory(test: Path => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-shard-storage")
    try test(directory)
    finally
      val paths = Files.walk(directory)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

final class ShardObjectStoreSuite extends FunSuite:
  import ShardStorageFixture.*

  test("different shards prepare concurrently and identical objects are forced only once") {
    withDirectory { root =>
      val store = ShardObjectStore(root)
      val executor = Executors.newFixedThreadPool(8)
      try
        val results = (0 until 64).map { index => executor.submit(new Callable[ShardObjectRef]:
          def call(): ShardObjectRef = store.put(index % 8, Array[Byte](1, 2, 3))
        ) }.map(_.get(10L, TimeUnit.SECONDS))
        assertEquals(results.distinct.size, 8)
        assertEquals(store.snapshot.writtenObjects, 8L)
        assertEquals(store.snapshot.writtenBytes, 24L)
        assertEquals(store.snapshot.reusedObjects, 56L)
        results.foreach(ref => assertEquals(store.read(ref).toVector, Vector[Byte](1, 2, 3)))
      finally executor.shutdownNow(): Unit
    }
  }

  test("missing truncated corrupt and mis-namespaced objects fail closed") {
    withDirectory { root =>
      val store = ShardObjectStore(root)
      val ref = store.put(1, Array[Byte](1, 2, 3))
      val path = root.resolve(ref.fileName)
      Files.write(path, Array[Byte](1, 2))
      intercept[ProtocolException](store.read(ref))
      Files.write(path, Array[Byte](1, 2, 4))
      intercept[ProtocolException](store.read(ref))
      intercept[ProtocolException](store.put(1, Array[Byte](1, 2, 3)))
      Files.delete(path)
      intercept[ProtocolException](store.read(ref))
      intercept[ProtocolException](store.read(ref.copy(shard = 2)))
    }
  }

  test("reclamation preserves referenced objects and unrelated files") {
    withDirectory { root =>
      val store = ShardObjectStore(root)
      val retained = store.put(0, Array[Byte](1))
      val removed = store.put(1, Array[Byte](2, 3))
      val unrelated = root.resolve("operator-notes")
      Files.writeString(unrelated, "retain me")
      store.retain(Set(retained))
      assertEquals(store.read(retained).toVector, Vector[Byte](1))
      assert(!Files.exists(root.resolve(removed.fileName)))
      assert(Files.exists(unrelated))
      assertEquals(store.snapshot.reclaimedBytes, 2L)
      assertEquals(store.snapshot.liveBytes, 1L)
      assertEquals(ShardObjectStore(root).snapshot.liveBytes, 1L)
    }
  }

  test("reclamation validates all retained objects before deleting anything") {
    withDirectory { root =>
      val store = ShardObjectStore(root)
      val ref = store.put(0, Array[Byte](1))
      intercept[ProtocolException](store.retain(Set(ref.copy(shard = 1))))
      assertEquals(store.read(ref).toVector, Vector[Byte](1))
    }
  }

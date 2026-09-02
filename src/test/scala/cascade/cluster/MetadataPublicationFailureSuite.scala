package cascade.cluster

import java.nio.file.Files
import munit.FunSuite

final class MetadataPublicationFailureSuite extends FunSuite:
  test("a checkpoint failure after publication fences retries until recovery") {
    ShardStorageFixture.withDirectory { root =>
      val path = root.resolve("metadata.log")
      val store = MetadataStore(path, 1024L)
      var last = ShardStorageFixture.active
      try
        store.commit(last)
        val blocker = path.resolveSibling("metadata.log.cleaned")
        Files.createDirectory(blocker)
        Files.writeString(blocker.resolve("fault"), "prevent replacement")
        var failed = false
        while !failed do
          last = MetadataDeltaFixture.update(last, "workers", last.version)._2
          try store.commit(last)
          catch case _: java.io.IOException => failed = true
        assert(!store.isHealthy)
        assertEquals(store.metadata, last)
        intercept[IllegalStateException](store.commit(last.copy(unavailableBrokerIds = Set(2))))
        Files.delete(blocker.resolve("fault"))
        Files.delete(blocker)
      finally store.close()
      val recovered = MetadataStore(path)
      try
        assert(recovered.isHealthy)
        assertEquals(recovered.metadata, last)
      finally recovered.close()
    }
  }

package cascade.backup

import cascade.cluster.{MetadataDeltaFixture, MetadataStore, ShardObjectStore, ShardStorageFixture}
import java.nio.file.Files
import munit.FunSuite

final class ShardStorageBackupSuite extends FunSuite:
  test("a consistent backup restores the journal and every referenced shard object") {
    ShardStorageFixture.withDirectory { root =>
      val source = root.resolve("source")
      val path = source.resolve(".cascade/cluster-metadata.log")
      val next = MetadataDeltaFixture.update(ShardStorageFixture.active)._2
      val store = MetadataStore(path)
      try
        store.commit(ShardStorageFixture.active)
        store.commit(next)
      finally store.close()
      // The source has no writer; exercise the same recursive copy used under the online write barrier.
      val backup = root.resolve("backup")
      val manifest = BackupCreator.createOnline(source, backup)
      assert(manifest.entries.exists(_.relativePath.endsWith("cluster-metadata.log")))
      assert(manifest.entries.count(_.relativePath.contains("cluster-metadata.log.shards/")) >= 3)
      val restored = root.resolve("restored")
      BackupRestore.restore(backup, restored)
      val recovered = MetadataStore(restored.resolve(".cascade/cluster-metadata.log"))
      try assertEquals(recovered.metadata, next)
      finally recovered.close()
      val entry = manifest.entries.find(_.relativePath.contains(".shards/")).get
      Files.delete(entry.resolveUnder(backup))
      intercept[IllegalArgumentException](BackupRestore.verify(backup)): Unit
    }
  }

  test("a bare journal copy is rejected rather than treated as a complete backup") {
    ShardStorageFixture.withDirectory { root =>
      val path = root.resolve("source/metadata.log")
      val store = MetadataStore(path)
      try store.commit(ShardStorageFixture.active)
      finally store.close()
      val target = root.resolve("incomplete/metadata.log")
      Files.createDirectories(target.getParent)
      Files.copy(path, target)
      intercept[Exception](MetadataStore(target))
      assert(Files.exists(ShardObjectStore.pathFor(path)))
    }
  }

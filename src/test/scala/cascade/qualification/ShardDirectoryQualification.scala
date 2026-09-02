package cascade.qualification

import cascade.cluster.{MetadataDeltaFixture, MetadataStore, ShardStorageFixture}

/** Run on the deployment filesystem; fails rather than skipping unsupported directory forcing. */
object ShardDirectoryQualification:
  def main(arguments: Array[String]): Unit =
    require(arguments.isEmpty, "this probe takes no arguments")
    ShardStorageFixture.withDirectory { root =>
      val path = root.resolve("metadata.log")
      val store = MetadataStore(path, 1024L)
      var current = ShardStorageFixture.active
      try
        store.commit(current)
        require(store.objectSnapshot.directoryForceSupported, "directory forcing is unavailable on this filesystem")
        (1 to 100).foreach { index =>
          current = MetadataDeltaFixture.update(current, "durable-workers", index.toLong)._2
          store.commit(current)
        }
        val objects = store.objectSnapshot
        require(objects.reclaimedObjects > 0L && objects.reclaimedBytes > 0L, "automatic reclamation was not exercised")
        println(s"SHARD_DIRECTORY_RESULT directory_force=true reclaimed_objects=${objects.reclaimedObjects} reclaimed_bytes=${objects.reclaimedBytes} stored_bytes=${objects.liveBytes}")
      finally store.close()
      val recovered = MetadataStore(path, 1024L)
      try require(recovered.metadata == current, "checkpoint/reclamation lost committed coordinator state")
      finally recovered.close()
    }

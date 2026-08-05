package cascade.cluster

import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class MetadataStoreSuite extends FunSuite:
  test("committed metadata images recover and checksum-corrupt tails are discarded") {
    val directory = Files.createTempDirectory("cascade-metadata-store-test")
    val path = directory.resolve("metadata.log")
    val first = ClusterMetadata(
      1L,
      Vector(TopicMetadata("events", Vector(PartitionMetadata(0, 1, 0, Vector(1, 2, 3), Vector(1, 2, 3)))))
    )
    val second = ClusterMetadata(
      2L,
      Vector(TopicMetadata("events", Vector(PartitionMetadata(0, 2, 1, Vector(1, 2, 3), Vector(2, 3)))))
    )
    try
      val store = MetadataStore(path)
      val firstFrameSize =
        try
          store.commit(first)
          val size = Files.size(path)
          store.commit(second)
          size
        finally store.close()

      val journal = Files.readAllBytes(path)
      journal(journal.length - 1) = (journal.last ^ 0xff).toByte
      Files.write(path, journal, StandardOpenOption.TRUNCATE_EXISTING)

      val recovered = MetadataStore(path)
      try
        assertEquals(recovered.metadata, first)
        assertEquals(Files.size(path), firstFrameSize)
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

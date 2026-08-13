package cascade.cluster

import cascade.protocol.ByteWriter
import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class MetadataStoreSuite extends FunSuite:
  test("committed metadata images recover and checksum-corrupt tails are discarded") {
    val directory = Files.createTempDirectory("cascade-metadata-store-test")
    val path = directory.resolve("metadata.log")
    val first = ClusterMetadata(
      1L,
      Vector(TopicMetadata("events", Vector(PartitionMetadata(0, 1, 0, Vector(1, 2, 3), Vector(1, 2, 3))))),
      controllerTerm = 4L
    )
    val second = ClusterMetadata(
      2L,
      Vector(TopicMetadata("events", Vector(PartitionMetadata(0, 2, 1, Vector(1, 2, 3), Vector(2, 3))))),
      controllerTerm = 4L
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

  test("version one metadata images remain readable with controller term zero") {
    val legacy = ByteWriter()
      .writeShort(1)
      .writeLong(7L)
      .writeArray(Vector.empty[TopicMetadata])(_ => ())
      .result()

    assertEquals(MetadataCodec.decode(legacy), ClusterMetadata(7L, Vector.empty, controllerTerm = 0L))
  }

  test("reassignment state round-trips and version two images default it to empty") {
    val reassigning = ClusterMetadata(
      9L,
      Vector(
        TopicMetadata(
          "events",
          Vector(
            PartitionMetadata(
              0,
              1,
              4,
              Vector(3, 2, 1),
              Vector(1, 2),
              addingReplicas = Vector(3),
              removingReplicas = Vector(1)
            )
          )
        )
      ),
      controllerTerm = 6L
    )
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(reassigning)), reassigning)

    val writer = ByteWriter()
      .writeShort(2)
      .writeLong(9L)
      .writeLong(6L)
    writer.writeArray(Vector("events")) { name =>
      writer.writeString(name)
      writer.writeArray(Vector(0)) { partition =>
        writer.writeInt(partition)
        writer.writeInt(1)
        writer.writeInt(4)
        writer.writeArray(Vector(1, 2))(writer.writeInt)
        writer.writeArray(Vector(1, 2))(writer.writeInt): Unit
      }: Unit
    }
    assertEquals(
      MetadataCodec.decode(writer.result()),
      ClusterMetadata(
        9L,
        Vector(TopicMetadata("events", Vector(PartitionMetadata(0, 1, 4, Vector(1, 2), Vector(1, 2))))),
        6L
      )
    )
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

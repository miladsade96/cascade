package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.storage.{CleanupPolicy, TopicLifecyclePolicy}
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

  test("stable and joint voter membership round-trip with endpoints and directory IDs") {
    val initial = QuorumMembership.bootstrap(
      Vector(ClusterNode(1, "node-1", 9092), ClusterNode(2, "node-2", 9093), ClusterNode(3, "node-3", 9094))
    )
    val added = QuorumVoter(ClusterNode(4, "node-4", 9095), VoterDirectoryId(11L, 12L))
    val metadata = ClusterMetadata(
      12L,
      Vector.empty,
      controllerTerm = 7L,
      membership = Some(initial.beginTransition(initial.currentVoters :+ added))
    )

    assertEquals(MetadataCodec.decode(MetadataCodec.encode(metadata)), metadata)
    assertEquals(
      MetadataCodec.decode(MetadataCodec.encode(metadata.copy(membership = Some(metadata.membership.get.stabilize)))),
      metadata.copy(membership = Some(metadata.membership.get.stabilize))
    )
  }

  test("version three metadata images default membership to the configured bootstrap state") {
    val legacy = ByteWriter()
      .writeShort(3)
      .writeLong(10L)
      .writeLong(8L)
      .writeArray(Vector.empty[TopicMetadata])(_ => ())
      .result()

    assertEquals(MetadataCodec.decode(legacy), ClusterMetadata(10L, Vector.empty, 8L, membership = None))
  }

  test("coordinator images round-trip and version four defaults them to empty") {
    val coordinator = CoordinatorMetadata(
      version = 17L,
      ownerTerm = 9L,
      groupState = Vector[Byte](1, 2, 3),
      deliveryState = Vector[Byte](4, 5, 6)
    )
    val metadata = ClusterMetadata(21L, Vector.empty, 9L, coordinator = coordinator)
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(metadata)), metadata)

    val legacy = ByteWriter()
      .writeShort(4)
      .writeLong(20L)
      .writeLong(8L)
      .writeArray(Vector.empty[TopicMetadata])(_ => ())
      .writeBoolean(false)
      .result()
    assertEquals(
      MetadataCodec.decode(legacy),
      ClusterMetadata(20L, Vector.empty, 8L, membership = None, coordinator = CoordinatorMetadata.Empty)
    )
  }

  test("per-topic lifecycle policy round-trips in the quorum metadata image") {
    val policy = TopicLifecyclePolicy(CleanupPolicy.CompactDelete, 3_600_000L, 1_073_741_824L)
    val metadata = ClusterMetadata(
      22L,
      Vector(TopicMetadata("orders", Vector(PartitionMetadata(0, 1, 2, Vector(1, 2, 3), Vector(1, 2))), Some(policy))),
      controllerTerm = 10L
    )
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(metadata)), metadata)
  }

  test("metadata encoding honors the rolling-upgrade format floor") {
    val versionFive = ClusterMetadata(
      23L,
      Vector(TopicMetadata("orders", Vector(PartitionMetadata(0, 1, 0, Vector(1), Vector(1))))),
      controllerTerm = 11L,
      membership = Some(QuorumMembership.bootstrap(Vector(ClusterNode(1, "node-1", 9092))))
    )
    val encoded = MetadataCodec.encode(versionFive, 5)
    assertEquals(ByteCursor(encoded).readShort(), 5.toShort)
    assertEquals(MetadataCodec.decode(encoded), versionFive)

    val requiringSix = versionFive.copy(
      topics = versionFive.topics.map(_.copy(lifecyclePolicy = Some(TopicLifecyclePolicy(CleanupPolicy.Compact, -1L, -1L))))
    )
    val error = intercept[ProtocolException](MetadataCodec.encode(requiringSix, 5))
    assert(error.getMessage.contains("requires format 6"))
  }

  test("rolling capability negotiation selects only common formats and features") {
    val old = PeerCapabilities("1.0.0", 1, 6, Map("online-snapshot" -> 0.toShort))
    val current = PeerCapabilities("1.1.0", 1, 7, Map("online-snapshot" -> 1.toShort, "consumer-v2" -> 1.toShort))
    assertEquals(
      NegotiatedCapabilities.across(Vector(old, current)),
      Right(NegotiatedCapabilities(6, Map.empty))
    )

    val incompatible = PeerCapabilities("0.8.0", 1, 4, Map.empty)
    assert(NegotiatedCapabilities.across(Vector(incompatible, current)).isRight)
    assertEquals(NegotiatedCapabilities.across(Vector(PeerCapabilities("future", 8, 9, Map.empty), current)).isLeft, true)
  }

  test("metadata format seven persists quorum-activated feature levels") {
    val metadata = ClusterMetadata(
      24L,
      Vector.empty,
      controllerTerm = 12L,
      featureLevels = Map(
        ClusterFeature.CoordinatorSharding -> 1.toShort,
        ClusterFeature.ConsumerProtocol -> 1.toShort
      )
    )

    assertEquals(MetadataCodec.minimumRequiredFormat(metadata), 7.toShort)
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(metadata, 7)), metadata)
    intercept[ProtocolException](MetadataCodec.encode(metadata, 6))
  }

  test("metadata format eight persists coordinator failover eligibility") {
    val metadata = ClusterMetadata(
      25L,
      Vector.empty,
      controllerTerm = 13L,
      featureLevels = Map(
        ClusterFeature.CoordinatorSharding -> 1.toShort,
        ClusterFeature.CoordinatorFailover -> 1.toShort
      ),
      unavailableBrokerIds = Set(1, 4)
    )

    assertEquals(MetadataCodec.minimumRequiredFormat(metadata), 8.toShort)
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(metadata, 8)), metadata)
    intercept[ProtocolException](MetadataCodec.encode(metadata, 7))
  }

  test("coordinator rendezvous sharding distributes keys and minimizes membership movement") {
    val three = Vector(
      ClusterNode(1, "node-1", 9092),
      ClusterNode(2, "node-2", 9093),
      ClusterNode(3, "node-3", 9094)
    )
    val keys = (0 until 10_000).map(index => s"tenant-$index").toVector
    val owners = keys.map(key => CoordinatorRouting.owner(key, three).get.id)
    val counts = owners.groupMapReduce(identity)(_ => 1)(_ + _)
    assertEquals(counts.keySet, Set(1, 2, 3))
    assert(counts.values.forall(count => count > 2800 && count < 3900), counts)

    val four = three :+ ClusterNode(4, "node-4", 9095)
    val moved = keys.count(key => CoordinatorRouting.owner(key, three) != CoordinatorRouting.owner(key, four))
    val movedToNewNode = keys.count(key =>
      CoordinatorRouting.owner(key, three) != CoordinatorRouting.owner(key, four) &&
        CoordinatorRouting.owner(key, four).exists(_.id == 4)
    )
    assertEquals(moved, movedToNewNode)
    assert(moved > 1800 && moved < 3200, moved)
  }

  test("metadata journal compaction bounds full-image history and recovers the latest image") {
    val directory = Files.createTempDirectory("cascade-metadata-compaction-test")
    val path = directory.resolve("metadata.log")
    try
      val store = MetadataStore(path, compactionBytes = 1024L)
      val latest =
        try
          (1L to 80L).map { version =>
            val image = ClusterMetadata(version, Vector.empty, controllerTerm = version)
            store.commit(image)
            image
          }.last
        finally store.close()
      assert(Files.size(path) < 1024L)

      val recovered = MetadataStore(path)
      try assertEquals(recovered.metadata, latest)
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

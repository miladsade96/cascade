package cascade.delivery

import cascade.protocol.ByteWriter
import cascade.storage.TopicPartition
import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class DeliveryStoreSuite extends FunSuite:
  test("delivery images round-trip every producer and transaction field") {
    val image = sampleImage(version = 7L, offsetsApplied = false)
    assertEquals(DeliveryCodec.decode(DeliveryCodec.encode(image)), image)
  }

  test("delivery codec reads headerless images written by previous releases") {
    val image = sampleImage(version = 6L, offsetsApplied = true)
    assertEquals(DeliveryCodec.decode(legacyBytes(image)), image)
  }

  test("delivery codec persists the active transaction protocol version") {
    val base = sampleImage(version = 8L, offsetsApplied = true)
    val v2 = base.copy(activeTransactions = base.activeTransactions.map(_.copy(protocolVersion = TransactionProtocol.Version2)))
    assertEquals(DeliveryCodec.decode(DeliveryCodec.encode(v2)), v2)
    assertEquals(DeliveryCodec.decode(legacyBytes(base)).activeTransactions.head.protocolVersion, TransactionProtocol.Version1)
  }

  test("delivery codec persists transaction v2 completion identities") {
    val base = sampleImage(version = 9L, offsetsApplied = true)
    val completed = base.completedTransactions.head.copy(
      protocolVersion = TransactionProtocol.Version2,
      resultingProducerId = 7L,
      resultingProducerEpoch = 2
    )
    val v2 = base.copy(completedTransactions = Vector(completed))
    assertEquals(DeliveryCodec.decode(DeliveryCodec.encode(v2)), v2)
    val legacy = DeliveryCodec.decode(legacyBytes(base)).completedTransactions.head
    assertEquals(legacy.protocolVersion, TransactionProtocol.Version1)
    assertEquals(legacy.resultingProducerId, -1L)
    assertEquals(legacy.resultingProducerEpoch, -1.toShort)
  }

  test("recovery truncates a partial delivery-image tail") {
    val directory = Files.createTempDirectory("cascade-delivery-partial")
    val path = directory.resolve("delivery.log")
    val expected = sampleImage(version = 1L, offsetsApplied = true)
    try
      val store = DeliveryStore(path)
      val completeSize =
        try
          store.commit(expected)
          Files.size(path)
        finally store.close()
      Files.write(path, Array[Byte](0, 0, 0), StandardOpenOption.APPEND)

      val recovered = DeliveryStore(path)
      try
        assertEquals(recovered.image, expected)
        assertEquals(Files.size(path), completeSize)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("recovery discards a checksum-corrupt tail without losing the prior image") {
    val directory = Files.createTempDirectory("cascade-delivery-checksum")
    val path = directory.resolve("delivery.log")
    val first = sampleImage(version = 1L, offsetsApplied = false)
    val second = sampleImage(version = 2L, offsetsApplied = true)
    try
      val store = DeliveryStore(path)
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

      val recovered = DeliveryStore(path)
      try
        assertEquals(recovered.image, first)
        assertEquals(Files.size(path), firstFrameSize)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("a quorum image can replace tentative memory without changing the local journal") {
    val directory = Files.createTempDirectory("cascade-delivery-snapshot")
    val path = directory.resolve("delivery.log")
    val durable = DeliveryImage.Empty.copy(version = 1L, nextProducerId = 3L)
    val tentative = durable.copy(version = 2L, nextProducerId = 4L)
    try
      val store = DeliveryStore(path)
      try
        store.commit(durable)
        val journalSize = Files.size(path)
        store.commit(tentative, durable = false)
        assertEquals(store.image, tentative)
        assertEquals(Files.size(path), journalSize)
        store.install(durable)
        assertEquals(store.image, durable)
        assertEquals(Files.size(path), journalSize)
      finally store.close()

      val recovered = DeliveryStore(path)
      try assertEquals(recovered.image, durable)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("delivery journal compaction retains the latest complete image") {
    val directory = Files.createTempDirectory("cascade-delivery-compaction")
    val path = directory.resolve("delivery.log")
    try
      val store = DeliveryStore(path, compactionBytes = 1024L)
      val latest =
        try
          (1L to 30L).map { version =>
            val image = sampleImage(version, offsetsApplied = version % 2L == 0L)
            store.commit(image)
            image
          }.last
        finally store.close()
      assert(Files.size(path) < 2048L)

      val recovered = DeliveryStore(path)
      try assertEquals(recovered.image, latest)
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def sampleImage(version: Long, offsetsApplied: Boolean): DeliveryImage =
    val range = TransactionRange("events", 0, 10L, 19L)
    val offset = PendingOffset("workers", "events", 0, 20L, 3, Some("checkpoint"))
    DeliveryImage(
      version,
      nextProducerId = 9L,
      producers = Vector(ProducerRegistration(8L, 2, Some("orders-producer"), 30_000)),
      activeTransactions = Vector(
        ActiveTransaction(
          "orders-producer",
          8L,
          2,
          30_000,
          1000L,
          Vector(TopicPartition("events", 0)),
          Vector(range),
          Vector("workers"),
          Vector(offset)
        )
      ),
      completedTransactions = Vector(
        CompletedTransaction(
          "previous-producer",
          7L,
          1,
          committed = true,
          offsetsApplied,
          Vector(range),
          Vector(offset)
        )
      )
    )

  private def legacyBytes(image: DeliveryImage): Array[Byte] =
    val writer = ByteWriter().writeLong(image.version).writeLong(image.nextProducerId)
    def writeRange(value: TransactionRange): Unit =
      writer.writeString(value.topic).writeInt(value.partition).writeLong(value.firstOffset).writeLong(value.lastOffset): Unit
    def writeOffset(value: PendingOffset): Unit =
      writer.writeString(value.groupId).writeString(value.topic).writeInt(value.partition)
      writer.writeLong(value.offset).writeInt(value.leaderEpoch).writeNullableString(value.metadata): Unit
    writer.writeArray(image.producers) { producer =>
      writer.writeLong(producer.producerId).writeShort(producer.producerEpoch)
      writer.writeNullableString(producer.transactionalId).writeInt(producer.transactionTimeoutMillis): Unit
    }
    writer.writeArray(image.activeTransactions) { transaction =>
      writer.writeString(transaction.transactionalId).writeLong(transaction.producerId).writeShort(transaction.producerEpoch)
      writer.writeInt(transaction.timeoutMillis).writeLong(transaction.startedAtMillis)
      writer.writeArray(transaction.partitions)(value => writer.writeString(value.topic).writeInt(value.partition): Unit)
      writer.writeArray(transaction.ranges)(writeRange)
      writer.writeArray(transaction.groups)(value => writer.writeString(value): Unit)
      writer.writeArray(transaction.pendingOffsets)(writeOffset): Unit
    }
    writer.writeArray(image.completedTransactions) { transaction =>
      writer.writeString(transaction.transactionalId).writeLong(transaction.producerId).writeShort(transaction.producerEpoch)
      writer.writeBoolean(transaction.committed).writeBoolean(transaction.offsetsApplied)
      writer.writeArray(transaction.ranges)(writeRange)
      writer.writeArray(transaction.pendingOffsets)(writeOffset): Unit
    }
    writer.result()

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

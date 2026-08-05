package cascade.delivery

import cascade.storage.TopicPartition
import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class DeliveryStoreSuite extends FunSuite:
  test("delivery images round-trip every producer and transaction field") {
    val image = sampleImage(version = 7L, offsetsApplied = false)
    assertEquals(DeliveryCodec.decode(DeliveryCodec.encode(image)), image)
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

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

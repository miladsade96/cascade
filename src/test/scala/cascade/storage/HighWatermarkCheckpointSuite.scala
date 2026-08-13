package cascade.storage

import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class HighWatermarkCheckpointSuite extends FunSuite:
  test("the newest valid slot recovers") {
    val directory = Files.createTempDirectory("cascade-high-watermark-checkpoint")
    val path = directory.resolve("high-watermark.checkpoint")
    try
      val checkpoint = HighWatermarkCheckpoint(path)
      try
        assertEquals(checkpoint.offset, None)
        assert(!checkpoint.existed)
        checkpoint.persist(11L)
        checkpoint.persist(29L)
      finally checkpoint.close()

      val recovered = HighWatermarkCheckpoint(path)
      try
        assert(recovered.existed)
        assertEquals(recovered.offset, Some(29L))
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("a torn newest slot falls back to the previous forced slot") {
    val directory = Files.createTempDirectory("cascade-high-watermark-torn-slot")
    val path = directory.resolve("high-watermark.checkpoint")
    try
      val checkpoint = HighWatermarkCheckpoint(path)
      try
        checkpoint.persist(7L)
        checkpoint.persist(13L)
      finally checkpoint.close()

      val bytes = Files.readAllBytes(path)
      bytes(32 + 8) = (bytes(32 + 8) ^ 0xff).toByte
      Files.write(path, bytes, StandardOpenOption.TRUNCATE_EXISTING)

      val recovered = HighWatermarkCheckpoint(path)
      try assertEquals(recovered.offset, Some(7L))
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("checkpoint updates can move backwards during authoritative truncation") {
    val directory = Files.createTempDirectory("cascade-high-watermark-rewind")
    val path = directory.resolve("high-watermark.checkpoint")
    try
      val checkpoint = HighWatermarkCheckpoint(path)
      try
        checkpoint.persist(50L)
        checkpoint.persist(8L)
      finally checkpoint.close()

      val recovered = HighWatermarkCheckpoint(path)
      try assertEquals(recovered.offset, Some(8L))
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

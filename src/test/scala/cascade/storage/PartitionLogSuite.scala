package cascade.storage

import cascade.TestRecordBatch
import java.nio.file.Files
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class PartitionLogSuite extends FunSuite:
  test("assigns monotonic offsets, rolls segments, and recovers its index") {
    val directory = Files.createTempDirectory("cascade-log-test")
    try
      val log = PartitionLog(directory, maxSegmentBytes = 1024)
      try
        (0 until 20).foreach { expected =>
          val result = log.append(TestRecordBatch.single(), force = false)
          assertEquals(result.baseOffset, expected.toLong)
        }
        assertEquals(log.highWatermark, 20L)
        assertEquals(log.fetch(5L, 122).records.length, 122)
      finally log.close()

      val recovered = PartitionLog(directory, maxSegmentBytes = 1024)
      try
        assertEquals(recovered.highWatermark, 20L)
        val fetched = recovered.fetch(19L, 1024)
        assertEquals(fetched.records.length, 61)
        assertEquals(RecordBatch.baseOffset(fetched.records), 19L)
      finally recovered.close()
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()


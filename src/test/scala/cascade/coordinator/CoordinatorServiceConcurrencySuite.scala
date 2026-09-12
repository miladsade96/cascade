package cascade.coordinator

import cascade.delivery.DeliveryCoordinator
import cascade.group.{CommittedOffset, GroupCoordinator, GroupOffsetKey, OffsetCommitValue}
import cascade.protocol.Errors
import cascade.storage.{FlushPolicy, TopicRegistry}
import java.nio.file.{Files, Path}
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class CoordinatorServiceConcurrencySuite extends FunSuite:
  test("group and delivery mutations reach independent checkpoints concurrently") {
    val directory = Files.createTempDirectory("cascade-service-concurrency")
    val registry = TopicRegistry(directory.resolve("data"), 1024 * 1024, FlushPolicy.Sync)
    val groups = GroupCoordinator(directory.resolve("offsets.log"), durableLocal = false, scheduleExpiration = false)
    val delivery = DeliveryCoordinator(
      directory.resolve("delivery.log"), registry, groups, durableLocal = false, scheduleExpiration = false
    )
    val entered = CountDownLatch(2)
    val release = CountDownLatch(1)
    val checkpoint: CoordinatorCheckpoint = () =>
      entered.countDown()
      release.await(5L, TimeUnit.SECONDS)
    groups.attachCheckpoint(checkpoint)
    delivery.attachCheckpoint(checkpoint)
    val executor = Executors.newFixedThreadPool(2)
    try
      val groupCommit = executor.submit(() => groups.commitOffsets(
        "parallel-group",
        -1,
        "",
        Vector(OffsetCommitValue(
          GroupOffsetKey("parallel-group", "events", 0),
          CommittedOffset(10L, -1, None, committedAtMillis = 1L)
        ))
      ))
      val producer = executor.submit(() => delivery.initProducerId(Some("parallel-transaction"), 30000))
      assert(entered.await(5L, TimeUnit.SECONDS), "group and delivery checkpoints remained serialized")
      release.countDown()
      assertEquals(groupCommit.get(5L, TimeUnit.SECONDS), Errors.None)
      assertEquals(producer.get(5L, TimeUnit.SECONDS).errorCode, Errors.None)
    finally
      release.countDown()
      executor.shutdownNow(): Unit
      delivery.close()
      groups.close()
      registry.close()
      deleteTree(directory)
  }

  private def deleteTree(root: Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(path => Files.deleteIfExists(path): Unit)
      finally paths.close()

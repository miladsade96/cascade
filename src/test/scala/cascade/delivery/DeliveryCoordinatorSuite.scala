package cascade.delivery

import cascade.TestRecordBatch
import cascade.cluster.{ReplicatedAppendResult, ReplicatedAppender}
import cascade.group.{CommittedOffset, GroupCoordinator, GroupOffsetKey, OffsetCommitValue}
import cascade.protocol.Errors
import cascade.storage.{FlushPolicy, TopicPartition, TopicRegistry}
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class DeliveryCoordinatorSuite extends FunSuite:
  test("producer epochs fence old owners and survive coordinator restart") {
    val directory = Files.createTempDirectory("cascade-delivery-fencing")
    try
      withCoordinator(directory) { (delivery, _, _) =>
        val first = delivery.initProducerId(Some("orders"), 30_000)
        assertEquals(first.errorCode, Errors.None)
        assertEquals(first.producerEpoch, 0.toShort)

        val second = delivery.initProducerId(Some("orders"), 30_000)
        assertEquals(second.producerId, first.producerId)
        assertEquals(second.producerEpoch, 1.toShort)
        assertEquals(
          delivery.addPartitions("orders", first.producerId, first.producerEpoch, Vector(TopicPartition("events", 0))),
          Errors.ProducerFenced
        )
        assertEquals(
          delivery.addPartitions("orders", second.producerId, second.producerEpoch, Vector(TopicPartition("events", 0))),
          Errors.None
        )
      }

      withCoordinator(directory) { (delivery, _, _) =>
        val recovered = delivery.initProducerId(Some("orders"), 30_000)
        assertEquals(recovered.producerId, 1L)
        assertEquals(recovered.producerEpoch, 2.toShort)
        assert(delivery.image.completedTransactions.exists(transaction => !transaction.committed))
      }
    finally deleteTree(directory)
  }

  test("transactional offsets commit once and are not replayed over a newer offset") {
    val directory = Files.createTempDirectory("cascade-delivery-offsets")
    val key = GroupOffsetKey("workers", "events", 0)
    try
      withCoordinator(directory) { (delivery, groups, _) =>
        val producer = delivery.initProducerId(Some("processor"), 30_000)
        assertEquals(
          delivery.addPartitions("processor", producer.producerId, producer.producerEpoch, Vector(TopicPartition("events", 0))),
          Errors.None
        )
        assertEquals(
          delivery.addOffsets("processor", producer.producerId, producer.producerEpoch, "workers"),
          Errors.None
        )
        assertEquals(
          delivery.stageOffsets(
            "processor",
            producer.producerId,
            producer.producerEpoch,
            "workers",
            Vector(PendingOffset("workers", "events", 0, 12L, 4, Some("transactional")))
          ),
          Errors.None
        )
        assertEquals(delivery.endTransaction("processor", producer.producerId, producer.producerEpoch, committed = true), Errors.None)
        assertEquals(groups.fetchOffset(key).map(_.offset), Some(12L))
        assert(delivery.image.completedTransactions.forall(_.offsetsApplied))

        assertEquals(
          groups.commitOffsets(
            "workers",
            -1,
            "",
            Vector(OffsetCommitValue(key, CommittedOffset(99L, 5, Some("newer"), System.currentTimeMillis())))
          ),
          Errors.None
        )
      }

      withCoordinator(directory) { (delivery, groups, _) =>
        assertEquals(groups.fetchOffset(key).map(_.offset), Some(99L))
        assert(delivery.image.completedTransactions.forall(_.offsetsApplied))
      }
    finally deleteTree(directory)
  }

  test("startup completes only an interrupted transactional-offset application") {
    val directory = Files.createTempDirectory("cascade-delivery-offset-replay")
    val key = GroupOffsetKey("workers", "events", 0)
    val pending = PendingOffset("workers", "events", 0, 21L, 6, Some("recover"))
    try
      val store = DeliveryStore(directory.resolve("delivery.log"))
      try
        store.commit(
          DeliveryImage.Empty.copy(
            version = 1L,
            completedTransactions = Vector(
              CompletedTransaction(
                "interrupted",
                4L,
                2,
                committed = true,
                offsetsApplied = false,
                ranges = Vector.empty,
                pendingOffsets = Vector(pending)
              )
            )
          )
        )
      finally store.close()

      withCoordinator(directory) { (delivery, groups, _) =>
        assertEquals(groups.fetchOffset(key).map(_.offset), Some(21L))
        assert(delivery.image.completedTransactions.forall(_.offsetsApplied))
      }
    finally deleteTree(directory)
  }

  test("expired transactions abort and reject further state transitions") {
    val directory = Files.createTempDirectory("cascade-delivery-timeout")
    try
      withCoordinator(directory) { (delivery, _, _) =>
        val producer = delivery.initProducerId(Some("expiring"), 1)
        assertEquals(
          delivery.addPartitions("expiring", producer.producerId, producer.producerEpoch, Vector(TopicPartition("events", 0))),
          Errors.None
        )
        Thread.sleep(10L)
        assertEquals(
          delivery.addOffsets("expiring", producer.producerId, producer.producerEpoch, "workers"),
          Errors.InvalidTxnState
        )
        assertEquals(delivery.image.activeTransactions, Vector.empty)
        assertEquals(delivery.image.completedTransactions.map(_.committed), Vector(false))
      }
    finally deleteTree(directory)
  }

  test("commit waits for an in-flight append and keeps its reserved transaction range") {
    val directory = Files.createTempDirectory("cascade-delivery-inflight")
    try
      withCoordinator(directory) { (delivery, _, _) =>
        val producer = delivery.initProducerId(Some("concurrent"), 30_000)
        assertEquals(
          delivery.addPartitions("concurrent", producer.producerId, producer.producerEpoch, Vector(TopicPartition("events", 0))),
          Errors.None
        )

        val appendEntered = CountDownLatch(1)
        val releaseAppend = CountDownLatch(1)
        val endStarted = CountDownLatch(1)
        val appender = new ReplicatedAppender:
          override def append(
              topic: String,
              partition: Int,
              records: Array[Byte],
              acknowledgements: Short,
              timeoutMillis: Int
          ): ReplicatedAppendResult =
            appendEntered.countDown()
            if !releaseAppend.await(5L, TimeUnit.SECONDS) then throw IllegalStateException("append release timed out")
            ReplicatedAppendResult(Errors.None, 0L)

        val executor = Executors.newFixedThreadPool(2)
        try
          val appendResult = executor.submit[DeliveryAppendResult](() =>
            delivery.append(
              Some("concurrent"),
              "events",
              0,
              TestRecordBatch.producer(producer.producerId, producer.producerEpoch, 0, transactional = true),
              -1,
              30_000,
              appender
            )
          )
          assert(appendEntered.await(5L, TimeUnit.SECONDS))

          val endResult = executor.submit[Short](() =>
            endStarted.countDown()
            delivery.endTransaction("concurrent", producer.producerId, producer.producerEpoch, committed = true)
          )
          assert(endStarted.await(5L, TimeUnit.SECONDS))
          Thread.sleep(50L)
          assert(!endResult.isDone, "EndTxn completed before its in-flight append")

          releaseAppend.countDown()
          assertEquals(appendResult.get(5L, TimeUnit.SECONDS), DeliveryAppendResult(Errors.None, 0L))
          assertEquals(endResult.get(5L, TimeUnit.SECONDS), Errors.None)
          val completed = delivery.image.completedTransactions.last
          assertEquals(completed.ranges, Vector(TransactionRange("events", 0, 0L, 0L)))
          assert(completed.committed)
        finally
          releaseAppend.countDown()
          executor.shutdownNow(): Unit
          executor.awaitTermination(5L, TimeUnit.SECONDS): Unit
      }
    finally deleteTree(directory)
  }

  test("producer fencing and active transactions continue from an installed snapshot") {
    val directory = Files.createTempDirectory("cascade-delivery-install")
    val registry = TopicRegistry(directory.resolve("data"), 1024 * 1024, FlushPolicy.Sync)
    val sourceGroups = GroupCoordinator(directory.resolve("source-offsets.log"), scheduleExpiration = false)
    val targetGroups = GroupCoordinator(directory.resolve("target-offsets.log"), scheduleExpiration = false)
    val source = DeliveryCoordinator(
      directory.resolve("source-delivery.log"),
      registry,
      sourceGroups,
      scheduleExpiration = false
    )
    val target = DeliveryCoordinator(
      directory.resolve("target-delivery.log"),
      registry,
      targetGroups,
      scheduleExpiration = false
    )
    try
      registry.getOrCreate("events")
      val producer = source.initProducerId(Some("failover-producer"), 30_000)
      assertEquals(
        source.addPartitions(
          "failover-producer",
          producer.producerId,
          producer.producerEpoch,
          Vector(TopicPartition("events", 0))
        ),
        Errors.None
      )

      target.installSnapshot(source.snapshotBytes.toVector)
      assertEquals(target.image.activeTransactions.map(_.transactionalId), Vector("failover-producer"))
      val fenced = target.initProducerId(Some("failover-producer"), 30_000)
      assertEquals(fenced.producerId, producer.producerId)
      assertEquals(fenced.producerEpoch, (producer.producerEpoch + 1).toShort)
      assertEquals(target.image.activeTransactions, Vector.empty)
      assert(target.image.completedTransactions.lastOption.exists(!_.committed))
    finally
      source.close()
      target.close()
      sourceGroups.close()
      targetGroups.close()
      registry.close()
      deleteTree(directory)
  }

  private def withCoordinator(directory: java.nio.file.Path)(
      test: (DeliveryCoordinator, GroupCoordinator, TopicRegistry) => Unit
  ): Unit =
    val registry = TopicRegistry(directory.resolve("data"), 1024 * 1024, FlushPolicy.Sync)
    val groups = GroupCoordinator(directory.resolve("offsets.log"))
    val delivery = DeliveryCoordinator(directory.resolve("delivery.log"), registry, groups)
    try
      registry.getOrCreate("events")
      test(delivery, groups, registry)
    finally
      delivery.close()
      groups.close()
      registry.close()

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

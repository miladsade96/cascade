package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
import cascade.group.{CommittedOffset, GroupCodec, GroupImage, GroupOffsetKey, GroupShardCodec, OffsetCommitValue}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.CopyOnWriteArrayList
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class CoordinatorImageInstallerSuite extends FunSuite:
  test("a blocked installer coalesces publications without dropping the newest image") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val complete = CountDownLatch(1)
    val versions = CopyOnWriteArrayList[Long]()
    val installer = CoordinatorImageInstaller { image =>
      if image.version == 1L then
        entered.countDown()
        // Do not call the suite's assertion machinery from the worker while the test thread waits on a latch.
        release.await()
      versions.add(image.version)
      if image.version == 1000L then complete.countDown()
    }
    try
      installer.offer(CoordinatorMetadata.Empty.copy(version = 1L))
      assert(entered.await(5L, TimeUnit.SECONDS))
      (2L to 1000L).foreach(v => installer.offer(CoordinatorMetadata.Empty.copy(version = v)))
      installer.offer(CoordinatorMetadata.Empty.copy(version = 3L))
      release.countDown()
      assert(complete.await(5L, TimeUnit.SECONDS))
      assertEquals(versions.asScala.toVector, Vector(1L, 1000L))
    finally
      release.countDown()
      installer.close()
  }

  test("equal global versions retain disjoint shard advances") {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val complete = CountDownLatch(1)
    val installed = CopyOnWriteArrayList[CoordinatorMetadata]()
    val baseline = CoordinatorMetadata.Empty
    val first = advanced(baseline, 1, 1L)
    val second = advanced(baseline, 2, 2L)
    val installer = CoordinatorImageInstaller { image =>
      if image.version == 1L && installed.isEmpty then
        entered.countDown()
        release.await()
      installed.add(image)
      if image.shardVersion(1) == 1L && image.shardVersion(2) == 1L then complete.countDown()
    }
    try
      installer.offer(first)
      assert(entered.await(5L, TimeUnit.SECONDS))
      installer.offer(first)
      installer.offer(second)
      release.countDown()
      assert(complete.await(5L, TimeUnit.SECONDS))
      val image = installed.asScala.last
      assertEquals(image.shardVersion(1), 1L)
      assertEquals(image.shardVersion(2), 1L)
    finally
      release.countDown()
      installer.close()
  }

  private def advanced(base: CoordinatorMetadata, shard: Int, offset: Long): CoordinatorMetadata =
    val group = Iterator.from(0).map(index => s"installer-$shard-$index").find(CoordinatorShard.group(_) == shard).get
    val value = OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, 1L))
    val image = GroupCodec.encode(GroupImage(1L, Vector.empty, Vector(value))).toVector
    val payload = GroupShardCodec.split(image)(shard)
    val update = CoordinatorShardUpdate(shard, base.shardVersion(shard), payload)
    CoordinatorShardState.merge(base, CoordinatorDelta(0L, Vector(update)), 0L).toOption.get

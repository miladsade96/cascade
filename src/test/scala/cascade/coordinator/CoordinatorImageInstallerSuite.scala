package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
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
        assert(release.await(5L, TimeUnit.SECONDS))
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

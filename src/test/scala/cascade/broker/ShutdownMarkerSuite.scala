package cascade.broker

import java.nio.file.Files
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class ShutdownMarkerSuite extends FunSuite:
  test("fresh, clean, and unclean starts are distinguished") {
    val directory = Files.createTempDirectory("cascade-shutdown-marker")
    try
      val fresh = ShutdownMarker(directory)
      assertEquals(fresh.beginRecovery(), RecoveryMode.Fresh)
      Files.write(directory.resolve("partition.log"), Array[Byte](1, 2, 3)): Unit

      val killed = ShutdownMarker(directory)
      assertEquals(killed.beginRecovery(), RecoveryMode.Unclean)
      killed.markClean()

      val clean = ShutdownMarker(directory)
      assertEquals(clean.beginRecovery(), RecoveryMode.Clean)
      assertEquals(clean.beginRecovery(), RecoveryMode.Unclean)
    finally deleteTree(directory)
  }

  test("a corrupt clean marker is treated as an unclean shutdown") {
    val directory = Files.createTempDirectory("cascade-corrupt-shutdown-marker")
    try
      Files.createDirectories(directory.resolve(".cascade")): Unit
      Files.write(directory.resolve(".cascade").resolve("clean-shutdown.marker"), Array[Byte](9, 9, 9)): Unit
      assertEquals(ShutdownMarker(directory).beginRecovery(), RecoveryMode.Unclean)
    finally deleteTree(directory)
  }

  test("KafkaBroker publishes clean restart state after its full close path") {
    val directory = Files.createTempDirectory("cascade-broker-clean-restart")
    try
      val first = KafkaBroker(BrokerConfig(bindHost = "127.0.0.1", port = 0, dataDirectory = directory))
      assertEquals(first.recoveryMode, RecoveryMode.Fresh)
      first.start()
      first.close()

      val second = KafkaBroker(BrokerConfig(bindHost = "127.0.0.1", port = 0, dataDirectory = directory))
      try assertEquals(second.recoveryMode, RecoveryMode.Clean)
      finally second.close()
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

package cascade.cluster

import cascade.protocol.ProtocolException
import java.nio.file.{Files, StandardOpenOption}
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class ControllerStateStoreSuite extends FunSuite:
  test("term and vote recover while a checksum-corrupt tail is discarded") {
    val directory = Files.createTempDirectory("cascade-controller-state-test")
    val path = directory.resolve("controller-state.log")
    try
      val store = ControllerStateStore(path)
      val recoverableSize =
        try
          store.persist(ControllerState(1L, None))
          store.persist(ControllerState(1L, Some(2)))
          val size = Files.size(path)
          store.persist(ControllerState(2L, None))
          size
        finally store.close()

      val journal = Files.readAllBytes(path)
      journal(journal.length - 1) = (journal.last ^ 0xff).toByte
      Files.write(path, journal, StandardOpenOption.TRUNCATE_EXISTING)

      val recovered = ControllerStateStore(path)
      try
        assertEquals(recovered.state, ControllerState(1L, Some(2)))
        assertEquals(Files.size(path), recoverableSize)
      finally recovered.close()
    finally deleteTree(directory)
  }

  test("a persisted vote cannot change within the same term") {
    val directory = Files.createTempDirectory("cascade-controller-vote-test")
    val store = ControllerStateStore(directory.resolve("controller-state.log"))
    try
      store.persist(ControllerState(5L, Some(1)))
      intercept[ProtocolException](store.persist(ControllerState(5L, Some(2))))
      assertEquals(store.state, ControllerState(5L, Some(1)))
    finally
      store.close()
      deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

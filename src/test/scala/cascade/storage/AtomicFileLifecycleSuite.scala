package cascade.storage

import java.nio.file.Files
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class AtomicFileLifecycleSuite extends FunSuite:
  test("startup purges a segment left in the renamed deletion state") {
    val directory = Files.createTempDirectory("cascade-atomic-delete-test")
    try
      val segment = directory.resolve("00000000000000000000.log")
      Files.write(segment, Array[Byte](1, 2, 3))
      val marked = AtomicFileLifecycle.markDeleted(segment)
      assert(!Files.exists(segment))
      assert(Files.exists(marked))

      AtomicFileLifecycle.recoverDeleted(directory)
      assert(!Files.exists(marked))
    finally deleteTree(directory)
  }

  test("atomically replaces a target with a completed temporary file") {
    val directory = Files.createTempDirectory("cascade-atomic-replace-test")
    try
      val target = directory.resolve("state.log")
      val temporary = directory.resolve("state.log.cleaned")
      Files.write(target, Array[Byte](1))
      Files.write(temporary, Array[Byte](2, 3))

      AtomicFileLifecycle.replace(temporary, target)
      assertEquals(Files.readAllBytes(target).toVector, Vector[Byte](2, 3))
      assert(!Files.exists(temporary))
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

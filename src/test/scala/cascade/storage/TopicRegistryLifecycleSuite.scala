package cascade.storage

import java.nio.file.Files
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class TopicRegistryLifecycleSuite extends FunSuite:
  test("persists and restores atomically acknowledged per-topic lifecycle policy") {
    val directory = Files.createTempDirectory("cascade-topic-policy")
    val policy = TopicLifecyclePolicy(CleanupPolicy.CompactDelete, 86_400_000L, 1_073_741_824L)
    try
      val first = TopicRegistry(directory, 1024 * 1024)
      try
        assertEquals(first.createTopic("orders", 2), CreateTopicResult.Created)
        assertEquals(first.configureLifecycle("orders", policy), Right(()))
        assertEquals(first.effectiveLifecyclePolicy("orders"), Some(policy))
      finally first.close()

      val second = TopicRegistry(directory, 1024 * 1024)
      try assertEquals(second.effectiveLifecyclePolicy("orders"), Some(policy))
      finally second.close()
    finally deleteTree(directory)
  }

  test("rejects malformed durable topic policy instead of silently using broker defaults") {
    val directory = Files.createTempDirectory("cascade-topic-policy-invalid")
    val metadata = directory.resolve(".cascade")
    Files.createDirectories(metadata)
    Files.writeString(metadata.resolve("topic-lifecycle.conf"), "orders unknown 10 20\n")
    try intercept[IllegalArgumentException](TopicRegistry(directory, 1024 * 1024)): Unit
    finally deleteTree(directory)
  }

  private def deleteTree(root: java.nio.file.Path): Unit =
    if Files.exists(root) then
      val paths = Files.walk(root)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
      finally paths.close()

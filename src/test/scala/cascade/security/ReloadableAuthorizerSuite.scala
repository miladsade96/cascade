package cascade.security

import java.nio.file.Files
import munit.FunSuite

final class ReloadableAuthorizerSuite extends FunSuite:
  test("bootstraps a deny-by-default ACL file for an authorized administrator") {
    val directory = Files.createTempDirectory("cascade-acl-bootstrap")
    val path = directory.resolve("security").resolve("acls.conf")
    try
      val authorizer = ReloadableAuthorizer(path, Set("User:cluster-admin"), reloadIntervalMillis = 60_000L)
      assert(Files.isRegularFile(path))
      assertEquals(Files.size(path), 0L)
      assert(authorizer.authorize("cluster-admin", AclOperation.Alter, Resource(ResourceType.Cluster, "cascade")))
      assert(!authorizer.authorize("application", AclOperation.Read, Resource(ResourceType.Topic, "events")))
    finally
      if Files.exists(path) then Files.delete(path)
      Files.delete(directory.resolve("security"))
      Files.delete(directory)
  }

  test("replaces ACL snapshots atomically and preserves the last valid rules") {
    val path = Files.createTempFile("cascade-reloadable-acls", ".conf")
    val resource = Resource(ResourceType.Topic, "orders")
    try
      Files.writeString(path, "allow alice Read Topic orders\n")
      val authorizer = ReloadableAuthorizer(path, Set.empty, reloadIntervalMillis = 60_000L)
      assert(authorizer.authorize("alice", AclOperation.Read, resource))
      assert(!authorizer.authorize("bob", AclOperation.Read, resource))

      Files.writeString(path, "allow bob Read Topic orders\n")
      assert(authorizer.reloadNow())
      assert(!authorizer.authorize("alice", AclOperation.Read, resource))
      assert(authorizer.authorize("bob", AclOperation.Read, resource))

      Files.writeString(path, "not-an-acl\n")
      assert(!authorizer.reloadNow())
      assert(authorizer.lastReloadError.nonEmpty)
      assert(authorizer.authorize("bob", AclOperation.Read, resource))
    finally Files.deleteIfExists(path): Unit
  }

  test("persists administrative creation and deletion before publishing the snapshot") {
    val path = Files.createTempFile("cascade-admin-acls", ".conf")
    try
      Files.writeString(path, "")
      val rule = AclRule(AclEffect.Allow, "User:alice", AclOperation.Write, ResourceType.Topic, "orders")
      val filter = AclFilter(
        Some(ResourceType.Topic), Some("orders"), AclPatternFilter.Literal, Some("User:alice"), Some("*"),
        Some(AclOperation.Write), Some(AclEffect.Allow)
      )
      val authorizer = ReloadableAuthorizer(path, Set.empty, reloadIntervalMillis = 60_000L)

      assertEquals(authorizer.createRules(Vector(rule)), Right(()))
      assert(authorizer.authorize("alice", AclOperation.Write, Resource(ResourceType.Topic, "orders"), "127.0.0.1"))
      assertEquals(AclAuthorizer.parse(path), Vector(rule))
      assertEquals(authorizer.deleteRules(filter), Right(Vector(rule)))
      assert(!authorizer.authorize("alice", AclOperation.Write, Resource(ResourceType.Topic, "orders"), "127.0.0.1"))
      assertEquals(AclAuthorizer.parse(path), Vector.empty)
    finally Files.deleteIfExists(path): Unit
  }

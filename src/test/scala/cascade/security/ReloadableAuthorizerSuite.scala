package cascade.security

import java.nio.file.Files
import munit.FunSuite

final class ReloadableAuthorizerSuite extends FunSuite:
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

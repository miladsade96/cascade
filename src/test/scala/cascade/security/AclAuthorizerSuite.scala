package cascade.security

import java.nio.file.Files
import munit.FunSuite

final class AclAuthorizerSuite extends FunSuite:
  test("authorizes exact, prefix, wildcard, and super-user rules") {
    val path = Files.createTempFile("cascade-acls", ".conf")
    try
      Files.writeString(
        path,
        """# effect principal operation resource-type resource-pattern
          |allow alice Write Topic orders-*
          |allow * Read Topic public
          |deny alice Write Topic orders-private
          |""".stripMargin
      )
      val authorizer = AclAuthorizer.load(path, Set("root"))

      assert(authorizer.authorize("alice", AclOperation.Write, Resource(ResourceType.Topic, "orders-eu")))
      assert(!authorizer.authorize("alice", AclOperation.Write, Resource(ResourceType.Topic, "orders-private")))
      assert(authorizer.authorize("bob", AclOperation.Read, Resource(ResourceType.Topic, "public")))
      assert(!authorizer.authorize("bob", AclOperation.Read, Resource(ResourceType.Topic, "private")))
      assert(authorizer.authorize("root", AclOperation.Alter, Resource(ResourceType.Cluster, "cascade")))
    finally Files.deleteIfExists(path): Unit
  }

  test("rejects malformed ACL records") {
    val path = Files.createTempFile("cascade-invalid-acls", ".conf")
    try
      Files.writeString(path, "allow alice Write Topic\n")
      intercept[IllegalArgumentException](AclAuthorizer.load(path, Set.empty))
    finally Files.deleteIfExists(path): Unit
  }

  test("distinguishes durable literal and prefix patterns and enforces principal and host identity") {
    val path = Files.createTempFile("cascade-typed-acls", ".conf")
    try
      val rules = Vector(
        AclRule(AclEffect.Allow, "User:alice", AclOperation.Write, ResourceType.Topic, "orders-*", AclPatternType.Literal, "10.0.0.1"),
        AclRule(AclEffect.Allow, "User:alice", AclOperation.Read, ResourceType.Topic, "events-", AclPatternType.Prefixed)
      )
      Files.writeString(path, AclAuthorizer.render(rules))
      val authorizer = AclAuthorizer.load(path, Set.empty)

      assert(authorizer.authorize("alice", AclOperation.Write, Resource(ResourceType.Topic, "orders-*"), "10.0.0.1"))
      assert(!authorizer.authorize("alice", AclOperation.Write, Resource(ResourceType.Topic, "orders-eu"), "10.0.0.1"))
      assert(!authorizer.authorize("alice", AclOperation.Write, Resource(ResourceType.Topic, "orders-*"), "10.0.0.2"))
      assert(authorizer.authorize("alice", AclOperation.Read, Resource(ResourceType.Topic, "events-eu"), "10.0.0.2"))
    finally Files.deleteIfExists(path): Unit
  }

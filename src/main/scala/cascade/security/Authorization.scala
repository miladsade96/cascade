package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

enum AclEffect:
  case Allow, Deny

object AclEffect:
  def parse(value: String): AclEffect = value.toLowerCase match
    case "allow" => AclEffect.Allow
    case "deny"  => AclEffect.Deny
    case other   => throw IllegalArgumentException(s"invalid ACL effect: $other")

enum AclOperation:
  case Read, Write, Create, Delete, Alter, Describe, ClusterAction, IdempotentWrite, All

object AclOperation:
  def parse(value: String): AclOperation =
    values.find(_.toString.equalsIgnoreCase(value)).getOrElse(throw IllegalArgumentException(s"invalid ACL operation: $value"))

enum ResourceType:
  case Topic, Group, Cluster, TransactionalId

object ResourceType:
  def parse(value: String): ResourceType =
    values.find(_.toString.equalsIgnoreCase(value)).getOrElse(throw IllegalArgumentException(s"invalid ACL resource type: $value"))

final case class AclRule(
    effect: AclEffect,
    principal: String,
    operation: AclOperation,
    resourceType: ResourceType,
    resourcePattern: String
):
  require(principal.nonEmpty, "ACL principal cannot be empty")
  require(resourcePattern.nonEmpty, "ACL resource pattern cannot be empty")

  def matches(candidatePrincipal: String, candidateOperation: AclOperation, resource: Resource): Boolean =
    (principal == "*" || principal == candidatePrincipal) &&
      (operation == AclOperation.All || operation == candidateOperation) &&
      resourceType == resource.resourceType &&
      matchesPattern(resourcePattern, resource.name)

  private def matchesPattern(pattern: String, value: String): Boolean =
    pattern == "*" || (pattern.endsWith("*") && value.startsWith(pattern.dropRight(1))) || pattern == value

final case class Resource(resourceType: ResourceType, name: String)

final class AclAuthorizer private (rules: Vector[AclRule], superUsers: Set[String]):
  def authorize(principal: String, operation: AclOperation, resource: Resource): Boolean =
    if superUsers.contains(principal) then true
    else
      val matching = rules.filter(_.matches(principal, operation, resource))
      !matching.exists(_.effect == AclEffect.Deny) && matching.exists(_.effect == AclEffect.Allow)

object AclAuthorizer:
  def load(path: Path, superUsers: Set[String]): AclAuthorizer =
    val rules = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.iterator.zipWithIndex.flatMap { case (raw, index) =>
      val line = raw.trim
      if line.isEmpty || line.startsWith("#") then None
      else
        val fields = line.split("\\s+", -1)
        if fields.length != 5 then throw IllegalArgumentException(s"invalid ACL at ${path.getFileName}:${index + 1}")
        Some(
          AclRule(
            AclEffect.parse(fields(0)),
            fields(1),
            AclOperation.parse(fields(2)),
            ResourceType.parse(fields(3)),
            fields(4)
          )
        )
    }.toVector
    AclAuthorizer(rules, superUsers)

  def allowAll: AclAuthorizer =
    AclAuthorizer(Vector(AclRule(AclEffect.Allow, "*", AclOperation.All, ResourceType.Cluster, "*")), Set.empty)

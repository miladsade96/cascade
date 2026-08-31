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

enum AclPatternType:
  case Literal, Prefixed

object AclPatternType:
  def parse(value: String): AclPatternType =
    values.find(_.toString.equalsIgnoreCase(value)).getOrElse(throw IllegalArgumentException(s"invalid ACL pattern type: $value"))

  def inferLegacy(pattern: String): (String, AclPatternType) =
    if pattern != "*" && pattern.endsWith("*") then pattern.dropRight(1) -> AclPatternType.Prefixed
    else pattern -> AclPatternType.Literal

enum AclPatternFilter:
  case Any, Match, Literal, Prefixed

final case class AclRule(
    effect: AclEffect,
    principal: String,
    operation: AclOperation,
    resourceType: ResourceType,
    resourcePattern: String,
    patternType: AclPatternType = AclPatternType.Literal,
    host: String = "*"
):
  require(principal.nonEmpty, "ACL principal cannot be empty")
  require(resourcePattern.nonEmpty, "ACL resource pattern cannot be empty")
  require(host.nonEmpty, "ACL host cannot be empty")
  require(Vector(principal, resourcePattern, host).forall(value => !value.exists(_.isWhitespace)), "ACL fields cannot contain whitespace")

  def matches(candidatePrincipal: String, candidateOperation: AclOperation, resource: Resource, candidateHost: String): Boolean =
    principalMatches(principal, candidatePrincipal) && hostMatches(host, candidateHost) &&
      (operation == AclOperation.All || operation == candidateOperation) && resourceType == resource.resourceType &&
      resourceMatches(resource.name)

  def matchesFilter(filter: AclFilter): Boolean =
    filter.resourceType.forall(_ == resourceType) && filter.resourceName.forall(nameMatchesFilter(_, filter.patternType)) &&
      filter.principal.forall(principalMatches(principal, _)) && filter.host.forall(hostMatches(host, _)) &&
      filter.operation.forall(candidate => candidate == AclOperation.All || candidate == operation) &&
      filter.effect.forall(_ == effect)

  private def resourceMatches(value: String): Boolean = patternType match
    case AclPatternType.Literal  => resourcePattern == "*" || resourcePattern == value
    case AclPatternType.Prefixed => value.startsWith(resourcePattern)

  private def nameMatchesFilter(name: String, filterType: AclPatternFilter): Boolean = filterType match
    case AclPatternFilter.Any      => resourcePattern == name
    case AclPatternFilter.Literal  => patternType == AclPatternType.Literal && resourcePattern == name
    case AclPatternFilter.Prefixed => patternType == AclPatternType.Prefixed && resourcePattern == name
    case AclPatternFilter.Match =>
      (patternType == AclPatternType.Literal && (resourcePattern == "*" || resourcePattern == name)) ||
        (patternType == AclPatternType.Prefixed && name.startsWith(resourcePattern))

  private def principalMatches(rulePrincipal: String, candidate: String): Boolean =
    rulePrincipal == "*" || canonicalPrincipal(rulePrincipal) == canonicalPrincipal(candidate)

  private def hostMatches(ruleHost: String, candidate: String): Boolean = ruleHost == "*" || ruleHost == candidate

  private def canonicalPrincipal(value: String): String = value.stripPrefix("User:")

final case class AclFilter(
    resourceType: Option[ResourceType],
    resourceName: Option[String],
    patternType: AclPatternFilter,
    principal: Option[String],
    host: Option[String],
    operation: Option[AclOperation],
    effect: Option[AclEffect]
)

final case class Resource(resourceType: ResourceType, name: String)

final class AclAuthorizer private (val rules: Vector[AclRule], superUsers: Set[String]):
  def authorize(principal: String, operation: AclOperation, resource: Resource, host: String = "*"): Boolean =
    if superUsers.exists(user => user == principal || user.stripPrefix("User:") == principal.stripPrefix("User:")) then true
    else
      val matching = rules.filter(_.matches(principal, operation, resource, host))
      !matching.exists(_.effect == AclEffect.Deny) && matching.exists(_.effect == AclEffect.Allow)

object AclAuthorizer:
  def load(path: Path, superUsers: Set[String]): AclAuthorizer = AclAuthorizer(parse(path), superUsers)

  def fromRules(rules: Vector[AclRule], superUsers: Set[String]): AclAuthorizer = AclAuthorizer(rules, superUsers)

  def parse(path: Path): Vector[AclRule] =
    Files.readAllLines(path, StandardCharsets.UTF_8).asScala.iterator.zipWithIndex.flatMap { case (raw, index) =>
      val line = raw.trim
      if line.isEmpty || line.startsWith("#") then None
      else
        val fields = line.split("\\s+", -1)
        if fields.length != 5 && fields.length != 7 then
          throw IllegalArgumentException(s"invalid ACL at ${path.getFileName}:${index + 1}")
        val (pattern, patternType, host) =
          if fields.length == 5 then
            val (legacyPattern, legacyType) = AclPatternType.inferLegacy(fields(4))
            (legacyPattern, legacyType, "*")
          else (fields(4), AclPatternType.parse(fields(5)), fields(6))
        Some(
          AclRule(
            AclEffect.parse(fields(0)),
            fields(1),
            AclOperation.parse(fields(2)),
            ResourceType.parse(fields(3)),
            pattern,
            patternType,
            host
          )
        )
    }.toVector

  def render(rules: Vector[AclRule]): String =
    rules.map { rule =>
      s"${rule.effect.toString.toLowerCase} ${rule.principal} ${rule.operation} ${rule.resourceType} " +
        s"${rule.resourcePattern} ${rule.patternType} ${rule.host}"
    }.mkString("", System.lineSeparator(), System.lineSeparator())

  def allowAll: AclAuthorizer =
    AclAuthorizer(Vector(AclRule(AclEffect.Allow, "*", AclOperation.All, ResourceType.Cluster, "*")), Set.empty)

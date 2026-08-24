package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import javax.security.auth.x500.X500Principal
import scala.jdk.CollectionConverters.*

final case class PeerIdentityPolicy private (byNodeId: Map[Int, Set[String]]):
  require(byNodeId.nonEmpty, "peer identity policy must not be empty")

  def authorize(nodeId: Int, principal: String): Boolean =
    try byNodeId.get(nodeId).exists(_.contains(PeerIdentityPolicy.canonical(principal)))
    catch case _: IllegalArgumentException => false

  def principals(nodeId: Int): Set[String] = byNodeId.getOrElse(nodeId, Set.empty)

  def nodeIds: Set[Int] = byNodeId.keySet

object PeerIdentityPolicy:
  def apply(entries: Vector[(Int, String)]): PeerIdentityPolicy =
    require(entries.nonEmpty, "peer identity policy must not be empty")
    entries.foreach { case (nodeId, principal) =>
      require(nodeId >= 0, "peer identity node ID must be non-negative")
      require(principal.nonEmpty, "peer identity principal must not be empty")
    }
    val canonicalEntries = entries.map { case (nodeId, principal) => nodeId -> canonical(principal) }
    val conflicting = canonicalEntries.groupBy(_._2).collectFirst {
      case (principal, values) if values.map(_._1).distinct.size > 1 => principal
    }
    require(conflicting.isEmpty, s"peer principal maps to multiple node IDs: ${conflicting.getOrElse("")}")
    new PeerIdentityPolicy(canonicalEntries.groupMap(_._1)(_._2).view.mapValues(_.toSet).toMap)

  private[security] def canonical(value: String): String =
    X500Principal(value).getName(X500Principal.RFC2253)

object PeerIdentityFile:
  def load(path: Path): PeerIdentityPolicy =
    val entries = Files.readAllLines(path, StandardCharsets.UTF_8).asScala.iterator.zipWithIndex.flatMap { case (raw, index) =>
      val line = raw.trim
      if line.isEmpty || line.startsWith("#") then None
      else
        val separator = line.indexWhere(_.isWhitespace)
        if separator <= 0 || separator == line.length - 1 then
          throw IllegalArgumentException(s"invalid peer identity at ${path.getFileName}:${index + 1}")
        val nodeId =
          try line.substring(0, separator).toInt
          catch case _: NumberFormatException =>
            throw IllegalArgumentException(s"invalid peer node ID at ${path.getFileName}:${index + 1}")
        val principal = line.substring(separator).trim
        Some(nodeId -> principal)
    }.toVector
    PeerIdentityPolicy(entries)


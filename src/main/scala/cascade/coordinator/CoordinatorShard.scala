package cascade.coordinator

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Layout 1 is a storage contract: changing these constants requires a new feature level. */
object CoordinatorShard:
  val Buckets = 64
  val Allocator = Buckets * 2
  val Count = Allocator + 1

  def group(groupId: String): Int = bucket(groupId)
  def transaction(transactionalId: String): Int = Buckets + bucket(transactionalId)
  def producer(producerId: Long, transactionalId: Option[String]): Int =
    transactionalId.fold(Buckets + bucket(s"producer:$producerId"))(transaction)

  def valid(id: Int): Boolean = id >= 0 && id < Count

  def routingKey(id: Int): String =
    require(valid(id), "invalid coordinator shard ID")
    s"cascade-coordinator-shard-$id"

  private def bucket(key: String): Int =
    val digest = MessageDigest.getInstance("SHA-256").digest(key.getBytes(StandardCharsets.UTF_8))
    (digest(0) & 0xff) % Buckets

enum CoordinatorDomain:
  case Group, Transaction

final case class CoordinatorKey(domain: CoordinatorDomain, value: String):
  require(value.nonEmpty, "coordinator key must not be empty")

  val shard: Int = domain match
    case CoordinatorDomain.Group       => CoordinatorShard.group(value)
    case CoordinatorDomain.Transaction => CoordinatorShard.transaction(value)

  val routingKey: String = CoordinatorShard.routingKey(shard)

object CoordinatorKey:
  def group(groupId: String): CoordinatorKey = CoordinatorKey(CoordinatorDomain.Group, groupId)
  def transaction(transactionalId: String): CoordinatorKey = CoordinatorKey(CoordinatorDomain.Transaction, transactionalId)

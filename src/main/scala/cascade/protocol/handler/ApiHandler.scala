package cascade.protocol.handler

import cascade.protocol.{ByteCursor, RequestHeader}
import cascade.security.ConnectionSession

type ApiHandlerFunction = (RequestHeader, ByteCursor, ConnectionSession) => Option[Array[Byte]]

/** One cohesive Kafka API domain registered with the broker dispatcher. */
trait ApiHandler:
  def apiKeys: Set[Short]
  def handle(apiKey: Short, header: RequestHeader, body: ByteCursor, session: ConnectionSession): Option[Array[Byte]]

abstract class DomainApiHandler private[handler] (private val routes: Map[Short, ApiHandlerFunction]) extends ApiHandler:
  require(routes.nonEmpty, "an API handler must own at least one API key")

  final override val apiKeys: Set[Short] = routes.keySet

  final override def handle(
      apiKey: Short,
      header: RequestHeader,
      body: ByteCursor,
      session: ConnectionSession
  ): Option[Array[Byte]] =
    routes.getOrElse(apiKey, throw IllegalArgumentException(s"API key $apiKey is not owned by ${getClass.getSimpleName}"))(
      header,
      body,
      session
    )

final class ProduceApiHandler(route: ApiHandlerFunction) extends DomainApiHandler(Map(0.toShort -> route))

final class FetchApiHandler(routes: Map[Short, ApiHandlerFunction]) extends DomainApiHandler(routes)

final class MetadataApiHandler(routes: Map[Short, ApiHandlerFunction]) extends DomainApiHandler(routes)

final class GroupApiHandler(routes: Map[Short, ApiHandlerFunction]) extends DomainApiHandler(routes)

final class TransactionApiHandler(routes: Map[Short, ApiHandlerFunction]) extends DomainApiHandler(routes)

final class AdminApiHandler(routes: Map[Short, ApiHandlerFunction]) extends DomainApiHandler(routes)

final class SecurityApiHandler(routes: Map[Short, ApiHandlerFunction]) extends DomainApiHandler(routes)

final class ApiHandlerRegistry private (private val handlers: Map[Short, ApiHandler]):
  def handlerFor(apiKey: Short): Option[ApiHandler] = handlers.get(apiKey)

  def apiKeys: Set[Short] = handlers.keySet

object ApiHandlerRegistry:
  def apply(domains: Iterable[ApiHandler]): ApiHandlerRegistry =
    val entries = domains.iterator.flatMap(domain => domain.apiKeys.iterator.map(_ -> domain)).toVector
    val duplicates = entries.groupMap(_._1)(_._2).collect { case (apiKey, owners) if owners.size > 1 => apiKey }.toVector.sorted
    require(duplicates.isEmpty, s"duplicate Kafka API handlers: ${duplicates.mkString(", ")}")
    new ApiHandlerRegistry(entries.toMap)

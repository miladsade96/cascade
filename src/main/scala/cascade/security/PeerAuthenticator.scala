package cascade.security

final case class AuthenticatedPeer(nodeId: Option[Int], principal: String, encrypted: Boolean)

final class PeerAuthenticator(config: PeerSecurityConfig):
  private val identities = config.identityFile.map(path => ReloadablePeerIdentities(path, config.identityReloadIntervalMillis))

  def authenticate(clientId: Option[String], session: ConnectionSession): Either[String, AuthenticatedPeer] =
    config.protocol match
      case PeerSecurityProtocol.Plaintext =>
        clientId match
          case Some("cascade-peer") => Right(AuthenticatedPeer(None, session.principal, encrypted = false))
          case Some(value) =>
            parseNodeId(value)
              .map(nodeId => AuthenticatedPeer(Some(nodeId), session.principal, encrypted = false))
              .toRight("invalid_peer_client_id")
          case None => Left("missing_peer_client_id")
      case PeerSecurityProtocol.Ssl =>
        if !session.secure then Left("peer_tls_required")
        else
          parseNodeId(clientId.getOrElse("")) match
            case None => Left("invalid_peer_client_id")
            case Some(nodeId) =>
              session.transportPrincipal match
                case None => Left("peer_certificate_required")
                case Some(principal) if identities.exists(_.authorize(nodeId, principal)) =>
                  Right(AuthenticatedPeer(Some(nodeId), principal, encrypted = true))
                case Some(_) => Left("peer_identity_denied")

  def lastReloadError: Option[String] = identities.flatMap(_.lastReloadError)

  private def parseNodeId(value: String): Option[Int] =
    val prefix = "cascade-peer:"
    if !value.startsWith(prefix) || value.length == prefix.length then None
    else
      try Option(value.substring(prefix.length).toInt).filter(_ >= 0)
      catch case _: NumberFormatException => None


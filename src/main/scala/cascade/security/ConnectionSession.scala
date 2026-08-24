package cascade.security

final class ConnectionSession(
    val remoteAddress: String,
    val secure: Boolean,
    authenticationRequired: Boolean,
    transportPrincipal: Option[String] = None
):
  @volatile private var currentPrincipal = transportPrincipal.getOrElse("ANONYMOUS")
  @volatile private var authenticatedState = !authenticationRequired
  @volatile private var mechanismState: Option[String] = None

  def principal: String = currentPrincipal

  def authenticated: Boolean = authenticatedState

  def mechanism: Option[String] = mechanismState

  def selectMechanism(value: String): Unit = synchronized {
    mechanismState = Some(value)
  }

  def authenticate(principal: String): Unit = synchronized {
    require(principal.nonEmpty, "authenticated principal cannot be empty")
    currentPrincipal = principal
    authenticatedState = true
  }

  def rejectAuthentication(): Unit = synchronized {
    currentPrincipal = "ANONYMOUS"
    authenticatedState = false
    mechanismState = None
  }

object ConnectionSession:
  val LocalAnonymous: ConnectionSession = ConnectionSession("local", secure = false, authenticationRequired = false)

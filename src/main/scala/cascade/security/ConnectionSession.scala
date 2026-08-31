package cascade.security

final class ConnectionSession(
    val remoteAddress: String,
    val secure: Boolean,
    authenticationRequired: Boolean,
    val transportPrincipal: Option[String] = None
):
  @volatile private var currentPrincipal = transportPrincipal.getOrElse("ANONYMOUS")
  @volatile private var currentAuthorizationPrincipals = transportPrincipal.toSet
  @volatile private var authenticatedState = !authenticationRequired
  @volatile private var mechanismState: Option[String] = None
  @volatile private var scramState: Option[ScramServerSession] = None
  @volatile private var authenticationExpiresAtMillis = Long.MaxValue
  @volatile private var terminateState = false
  private var pendingThrottleMillis = 0L

  def principal: String = currentPrincipal

  def authorizationPrincipals: Set[String] = currentAuthorizationPrincipals + currentPrincipal

  def authenticated: Boolean =
    expireAuthenticationIfNecessary()
    authenticatedState

  def mechanism: Option[String] = mechanismState

  def terminateRequested: Boolean = terminateState

  def addThrottle(delayMillis: Long): Unit = synchronized {
    pendingThrottleMillis = math.min(Int.MaxValue.toLong, Math.addExact(pendingThrottleMillis, math.max(0L, delayMillis)))
  }

  def consumeThrottleMillis(): Int = synchronized {
    val value = pendingThrottleMillis.toInt
    pendingThrottleMillis = 0L
    value
  }

  def selectMechanism(value: String): Unit = synchronized {
    mechanismState = Some(value)
    scramState = None
  }

  def selectScramMechanism(value: SaslMechanism, exchange: ScramServerSession): Unit = synchronized {
    require(value.scram, "SCRAM session requires a SCRAM mechanism")
    mechanismState = Some(value.wireName)
    scramState = Some(exchange)
  }

  def evaluateScram(token: Array[Byte]): Option[ScramStep] = synchronized(scramState.map(_.evaluate(token)))

  def authenticate(
      principal: String,
      expiresAtEpochMillis: Long = Long.MaxValue,
      roles: Set[String] = Set.empty
  ): Unit = synchronized {
    require(principal.nonEmpty, "authenticated principal cannot be empty")
    require(expiresAtEpochMillis > System.currentTimeMillis(), "authentication expiry must be in the future")
    require(roles.forall(role => role.nonEmpty && !role.exists(character => character.isWhitespace || character.isControl)), "authorization role is invalid")
    currentPrincipal = principal
    currentAuthorizationPrincipals = Set(principal) ++ roles.map(role => s"Role:$role")
    authenticatedState = true
    authenticationExpiresAtMillis = expiresAtEpochMillis
    scramState = None
  }

  def rejectAuthentication(): Unit = synchronized {
    currentPrincipal = "ANONYMOUS"
    currentAuthorizationPrincipals = transportPrincipal.toSet
    authenticatedState = false
    authenticationExpiresAtMillis = Long.MaxValue
    mechanismState = None
    scramState = None
    terminateState = true
  }

  def terminateAfterResponse(): Unit = terminateState = true

  private def expireAuthenticationIfNecessary(): Unit =
    if authenticatedState && authenticationExpiresAtMillis != Long.MaxValue &&
        System.currentTimeMillis() >= authenticationExpiresAtMillis
    then synchronized {
      if authenticatedState && authenticationExpiresAtMillis != Long.MaxValue &&
          System.currentTimeMillis() >= authenticationExpiresAtMillis
      then
        currentPrincipal = transportPrincipal.getOrElse("ANONYMOUS")
        currentAuthorizationPrincipals = transportPrincipal.toSet
        authenticatedState = false
        authenticationExpiresAtMillis = Long.MaxValue
        mechanismState = None
        scramState = None
    }

object ConnectionSession:
  val LocalAnonymous: ConnectionSession = ConnectionSession("local", secure = false, authenticationRequired = false)

package cascade.security

final class ConnectionSession(
    val remoteAddress: String,
    val secure: Boolean,
    authenticationRequired: Boolean,
    val transportPrincipal: Option[String] = None
):
  @volatile private var currentPrincipal = transportPrincipal.getOrElse("ANONYMOUS")
  @volatile private var authenticatedState = !authenticationRequired
  @volatile private var mechanismState: Option[String] = None
  @volatile private var scramState: Option[ScramServerSession] = None
  @volatile private var terminateState = false

  def principal: String = currentPrincipal

  def authenticated: Boolean = authenticatedState

  def mechanism: Option[String] = mechanismState

  def terminateRequested: Boolean = terminateState

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

  def authenticate(principal: String): Unit = synchronized {
    require(principal.nonEmpty, "authenticated principal cannot be empty")
    currentPrincipal = principal
    authenticatedState = true
    scramState = None
  }

  def rejectAuthentication(): Unit = synchronized {
    currentPrincipal = "ANONYMOUS"
    authenticatedState = false
    mechanismState = None
    scramState = None
    terminateState = true
  }

  def terminateAfterResponse(): Unit = terminateState = true

object ConnectionSession:
  val LocalAnonymous: ConnectionSession = ConnectionSession("local", secure = false, authenticationRequired = false)

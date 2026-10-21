package cascade.security

import munit.FunSuite

final class ConnectionSessionSuite extends FunSuite:
  test("keeps authentication state isolated to one connection") {
    val first = ConnectionSession("127.0.0.1", secure = true, authenticationRequired = true)
    val second = ConnectionSession("127.0.0.2", secure = true, authenticationRequired = true)

    first.selectMechanism("PLAIN")
    first.authenticate("alice")

    assert(first.authenticated)
    assertEquals(first.principal, "alice")
    assertEquals(first.mechanism, Some("PLAIN"))
    assert(!second.authenticated)
    assertEquals(second.principal, "ANONYMOUS")
  }

  test("uses a verified TLS identity when SASL is not required") {
    val session = ConnectionSession(
      "127.0.0.1",
      secure = true,
      authenticationRequired = false,
      transportPrincipal = Some("CN=client")
    )
    assert(session.authenticated)
    assertEquals(session.principal, "CN=client")
  }

  test("keeps an in-progress SCRAM exchange scoped to one connection and clears it on success") {
    val first = ConnectionSession("client-a", secure = true, authenticationRequired = true)
    val second = ConnectionSession("client-b", secure = true, authenticationRequired = true)
    val exchange = ScramServerSession(SaslMechanism.ScramSha256, _ => None)

    first.selectScramMechanism(SaslMechanism.ScramSha256, exchange)
    assertEquals(first.mechanism, Some("SCRAM-SHA-256"))
    assert(first.evaluateScram("n,,n=alice,r=nonce".getBytes).exists(_.isInstanceOf[ScramChallenge]))
    assertEquals(second.evaluateScram(Array.emptyByteArray), None)

    first.authenticate("alice")
    assertEquals(first.evaluateScram(Array.emptyByteArray), None)
    assert(first.authenticated)
  }

  test("expires a bearer-backed connection identity") {
    var nowMillis = 1_000L
    val session = ConnectionSession(
      "client",
      secure = true,
      authenticationRequired = true,
      nowMillis = () => nowMillis
    )
    session.selectMechanism("OAUTHBEARER")
    session.authenticate("alice", nowMillis + 10L)
    assert(session.authenticated)
    nowMillis += 10L
    assert(!session.authenticated)
    assertEquals(session.principal, "ANONYMOUS")
    assertEquals(session.mechanism, None)
  }

  test("keeps mapped roles separate from the authenticated audit principal") {
    val session = ConnectionSession("client", secure = true, authenticationRequired = true)
    session.authenticate("alice", roles = Set("publisher", "reader"))
    assertEquals(session.principal, "alice")
    assertEquals(session.authorizationPrincipals, Set("alice", "Role:publisher", "Role:reader"))
  }

  test("accumulates and consumes bounded protocol throttle time once") {
    val session = ConnectionSession("client", secure = false, authenticationRequired = false)
    session.addThrottle(12L)
    session.addThrottle(8L)
    assertEquals(session.consumeThrottleMillis(), 20)
    assertEquals(session.consumeThrottleMillis(), 0)
  }

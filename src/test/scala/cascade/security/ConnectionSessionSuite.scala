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

package cascade.protocol

import cascade.protocol.handler.*
import cascade.security.ConnectionSession
import munit.FunSuite

final class ApiHandlerRegistrySuite extends FunSuite:
  private val noResponse: ApiHandlerFunction = (_, _, _) => None

  test("registry routes each API key to its owning domain") {
    val produce = ProduceApiHandler(noResponse)
    val fetch = FetchApiHandler(Map(ApiKey.Fetch -> noResponse, ApiKey.ListOffsets -> noResponse))
    val registry = ApiHandlerRegistry(Vector(produce, fetch))

    assertEquals(registry.handlerFor(ApiKey.Produce), Some(produce))
    assertEquals(registry.handlerFor(ApiKey.Fetch), Some(fetch))
    assertEquals(registry.handlerFor(ApiKey.ListOffsets), Some(fetch))
    assertEquals(registry.handlerFor(ApiKey.Metadata), None)
  }

  test("registry rejects duplicate domain ownership") {
    val first = FetchApiHandler(Map(ApiKey.Fetch -> noResponse))
    val second = FetchApiHandler(Map(ApiKey.Fetch -> noResponse))
    val error = intercept[IllegalArgumentException](ApiHandlerRegistry(Vector(first, second)))
    assert(error.getMessage.contains(ApiKey.Fetch.toString))
  }

  test("domain handler rejects a key outside its contract") {
    val handler = FetchApiHandler(Map(ApiKey.Fetch -> noResponse))
    val header = RequestHeader(ApiKey.Fetch, 6, 1, Some("registry-test"))
    intercept[IllegalArgumentException] {
      handler.handle(ApiKey.Produce, header, ByteCursor(Array.emptyByteArray), ConnectionSession.LocalAnonymous)
    }
  }

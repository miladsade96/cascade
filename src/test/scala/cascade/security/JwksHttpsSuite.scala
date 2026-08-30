package cascade.security

import com.sun.net.httpserver.{HttpsConfigurator, HttpsServer}
import java.net.InetSocketAddress
import java.net.http.HttpClient
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.{KeyStore, SecureRandom}
import java.time.Duration
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import javax.net.ssl.{KeyManagerFactory, SSLContext, TrustManagerFactory}

final class JwksHttpsSuite extends munit.FunSuite:
  test("HTTPS JWKS fetching validates TLS, uses ETags, bounds bodies, and refuses redirects") {
    val directory = Files.createTempDirectory("cascade-jwks-https")
    val storePath = SecurityTestSupport.createKeyStore(directory)
    val keyStore = KeyStore.getInstance("PKCS12")
    val input = Files.newInputStream(storePath)
    try keyStore.load(input, SecurityTestSupport.StorePassword.toCharArray)
    finally input.close()
    val serverContext = SSLContext.getInstance("TLS")
    val keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm)
    keyManagers.init(keyStore, SecurityTestSupport.StorePassword.toCharArray)
    serverContext.init(keyManagers.getKeyManagers, null, SecureRandom())
    val clientContext = SSLContext.getInstance("TLS")
    val trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm)
    trustManagers.init(keyStore)
    clientContext.init(null, trustManagers.getTrustManagers, SecureRandom())

    val first = OAuthTestSupport.keyPair()
    val second = OAuthTestSupport.keyPair()
    val body = AtomicReference(OAuthTestSupport.jwks(Vector("first" -> first)).getBytes(StandardCharsets.UTF_8))
    val entityTag = AtomicReference("\"v1\"")
    val fail = AtomicBoolean(false)
    val server = HttpsServer.create(InetSocketAddress("127.0.0.1", 0), 0)
    server.setHttpsConfigurator(HttpsConfigurator(serverContext))
    server.createContext("/jwks", exchange =>
      try
        if fail.get() then exchange.sendResponseHeaders(503, -1L)
        else if Option(exchange.getRequestHeaders.getFirst("If-None-Match")).contains(entityTag.get()) then
          exchange.sendResponseHeaders(304, -1L)
        else
          val bytes = body.get()
          exchange.getResponseHeaders.set("Content-Type", "application/json")
          exchange.getResponseHeaders.set("ETag", entityTag.get())
          exchange.sendResponseHeaders(200, bytes.length.toLong)
          exchange.getResponseBody.write(bytes)
      finally exchange.close()
    )
    server.createContext("/redirect", exchange =>
      try
        exchange.getResponseHeaders.set("Location", "/jwks")
        exchange.sendResponseHeaders(302, -1L)
      finally exchange.close()
    )
    server.start()
    try
      val client = HttpClient.newBuilder()
        .sslContext(clientContext)
        .connectTimeout(Duration.ofSeconds(2))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build()
      val source = JwksSource(java.net.URI.create(s"https://localhost:${server.getAddress.getPort}/jwks"), 2000, Some(client))
      val firstFetch = source.fetch(None).asInstanceOf[JwksLoaded]
      assertEquals(firstFetch.entityTag, Some("\"v1\""))
      assertEquals(JwtKeySet.parse(firstFetch.bytes).keyIds, Set("first"))
      assertEquals(source.fetch(firstFetch.entityTag), JwksNotModified)

      body.set(OAuthTestSupport.jwks(Vector("second" -> second)).getBytes(StandardCharsets.UTF_8))
      entityTag.set("\"v2\"")
      val secondFetch = source.fetch(firstFetch.entityTag).asInstanceOf[JwksLoaded]
      assertEquals(JwtKeySet.parse(secondFetch.bytes).keyIds, Set("second"))

      fail.set(true)
      intercept[IllegalArgumentException](source.fetch(secondFetch.entityTag))
      fail.set(false)
      body.set(Array.fill[Byte](JwtKeySet.MaximumDocumentBytes + 1)('x'.toByte))
      intercept[IllegalArgumentException](source.fetch(None))

      val redirect = JwksSource(java.net.URI.create(s"https://localhost:${server.getAddress.getPort}/redirect"), 2000, Some(client))
      intercept[IllegalArgumentException](redirect.fetch(None))
    finally
      server.stop(0)
      SecurityTestSupport.deleteTree(directory)
  }

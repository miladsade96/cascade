package cascade.security

import java.nio.file.Files
import java.security.KeyStore

final class MutualTlsMaterialSuite extends munit.FunSuite:
  test("creates distinct CA-signed broker identities and a shared trust store") {
    val directory = Files.createTempDirectory("cascade-mutual-tls-material")
    try
      val material = SecurityTestSupport.createMutualTlsMaterial(directory, Vector(1, 2))
      assertEquals(material.principals.keySet, Set(1, 2))
      assertNotEquals(material.keyStores(1), material.keyStores(2))
      material.keyStores.values.foreach(path => assert(Files.size(path) > 0L))

      val trust = KeyStore.getInstance("PKCS12")
      val input = Files.newInputStream(material.trustStore)
      try trust.load(input, SecurityTestSupport.StorePassword.toCharArray)
      finally input.close()
      assert(trust.containsAlias("ca"))
    finally SecurityTestSupport.deleteTree(directory)
  }

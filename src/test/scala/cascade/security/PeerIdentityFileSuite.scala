package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.Files

final class PeerIdentityFileSuite extends munit.FunSuite:
  test("loads canonical X.500 identities and supports certificate overlap per node") {
    val file = Files.createTempFile("cascade-peer-identities", ".conf")
    try
      Files.writeString(
        file,
        """# node certificate identities
          |1 CN=broker-1, OU=Data, O=Cascade
          |1 CN=broker-1-next,OU=Data,O=Cascade
          |2 CN=broker-2,OU=Data,O=Cascade
          |""".stripMargin,
        StandardCharsets.UTF_8
      ): Unit

      val policy = PeerIdentityFile.load(file)
      assertEquals(policy.nodeIds, Set(1, 2))
      assert(policy.authorize(1, "CN=broker-1,OU=Data,O=Cascade"))
      assert(policy.authorize(1, "CN=broker-1-next,OU=Data,O=Cascade"))
      assert(!policy.authorize(2, "CN=broker-1,OU=Data,O=Cascade"))
      assert(!policy.authorize(1, "not-an-x500-name"))
    finally Files.deleteIfExists(file): Unit
  }

  test("rejects malformed entries, negative IDs, and one certificate assigned to two nodes") {
    val file = Files.createTempFile("cascade-peer-identities-invalid", ".conf")
    try
      Files.writeString(file, "missing-principal", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](PeerIdentityFile.load(file))

      Files.writeString(file, "-1 CN=broker\n", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](PeerIdentityFile.load(file))

      Files.writeString(file, "1 CN=shared\n2 CN=shared\n", StandardCharsets.UTF_8): Unit
      intercept[IllegalArgumentException](PeerIdentityFile.load(file))
    finally Files.deleteIfExists(file): Unit
  }

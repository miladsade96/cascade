package cascade.security

import java.nio.charset.StandardCharsets
import java.util.Arrays

final class ScramCredentialSuite extends munit.FunSuite:
  SaslMechanism.Supported.filter(_.scram).foreach { mechanism =>
    test(s"$mechanism verifies a client proof and returns the server signature") {
      val password = "correct horse battery staple".toCharArray
      val salt = Array.tabulate[Byte](16)(_.toByte)
      val credential = ScramCredential.fromPassword(mechanism, password, ScramCredential.MinimumIterations, salt)
      val authMessage = "n=alice,r=client,r=clientserver,s=AAECAwQFBgcICQoLDA0ODw==,i=4096,c=biws,r=clientserver"
      val salted = deriveForTest(mechanism, password, salt)
      val clientKey = ScramCredential.hmac(mechanism, salted, "Client Key".getBytes(StandardCharsets.US_ASCII))
      val signature = ScramCredential.hmac(mechanism, credential.storedKey, authMessage.getBytes(StandardCharsets.UTF_8))
      val proof = clientKey.indices.map(index => (clientKey(index) ^ signature(index)).toByte).toArray

      try
        val expected = ScramCredential.hmac(mechanism, credential.serverKey, authMessage.getBytes(StandardCharsets.UTF_8))
        assert(credential.authenticate(authMessage, proof).exists(Arrays.equals(_, expected)))
        proof(0) = (proof(0) ^ 1).toByte
        assertEquals(credential.authenticate(authMessage, proof), None)
      finally
        Arrays.fill(password, '\u0000')
        Arrays.fill(salted, 0.toByte)
        Arrays.fill(clientKey, 0.toByte)
        Arrays.fill(signature, 0.toByte)
        Arrays.fill(proof, 0.toByte)
    }
  }

  test("rejects weak or excessive work factors and malformed key material") {
    val password = "password".toCharArray
    try
      intercept[IllegalArgumentException](
        ScramCredential.fromPassword(SaslMechanism.ScramSha256, password, ScramCredential.MinimumIterations - 1, new Array[Byte](16))
      )
      intercept[IllegalArgumentException](
        ScramCredential.fromPassword(SaslMechanism.ScramSha256, password, ScramCredential.MaximumIterations + 1, new Array[Byte](16))
      )
      intercept[IllegalArgumentException](
        ScramCredential.fromKeys(SaslMechanism.ScramSha256, ScramCredential.MinimumIterations, new Array[Byte](15), new Array[Byte](32), new Array[Byte](32))
      )
    finally Arrays.fill(password, '\u0000')
  }

  private def deriveForTest(mechanism: SaslMechanism, password: Array[Char], salt: Array[Byte]): Array[Byte] =
    val algorithm = if mechanism == SaslMechanism.ScramSha256 then "PBKDF2WithHmacSHA256" else "PBKDF2WithHmacSHA512"
    val specification = javax.crypto.spec.PBEKeySpec(
      password,
      salt,
      ScramCredential.MinimumIterations,
      ScramCredential.keyBytes(mechanism) * 8
    )
    try javax.crypto.SecretKeyFactory.getInstance(algorithm).generateSecret(specification).getEncoded
    finally specification.clearPassword()

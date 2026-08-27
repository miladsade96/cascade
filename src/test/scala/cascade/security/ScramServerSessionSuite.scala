package cascade.security

import java.nio.charset.StandardCharsets
import java.util.{Arrays, Base64}

final class ScramServerSessionSuite extends munit.FunSuite:
  test("SCRAM-SHA-256 completes a signed two-step exchange") {
    val mechanism = SaslMechanism.ScramSha256
    val password = "server-session-password".toCharArray
    val salt = Array.tabulate[Byte](16)(_.toByte)
    val credential = ScramCredential.fromPassword(mechanism, password, ScramCredential.MinimumIterations, salt)
    val session = ScramServerSession(mechanism, user => Option.when(user == "alice")(credential), () => "SERVER")
    try
      val clientFirstBare = "n=alice,r=CLIENT"
      val challenge = session.evaluate(bytes(s"n,,$clientFirstBare")).asInstanceOf[ScramChallenge]
      val serverFirst = String(challenge.bytes, StandardCharsets.UTF_8)
      assert(serverFirst.startsWith("r=CLIENTSERVER,s="))
      val clientFinalWithoutProof = "c=biws,r=CLIENTSERVER"
      val authMessage = s"$clientFirstBare,$serverFirst,$clientFinalWithoutProof"
      val proof = clientProof(mechanism, password, salt, credential, authMessage)
      val success = session.evaluate(bytes(s"$clientFinalWithoutProof,p=${Base64.getEncoder.encodeToString(proof)}"))
        .asInstanceOf[ScramSuccess]

      assertEquals(success.principal, "alice")
      val expected = ScramCredential.hmac(mechanism, credential.serverKey, bytes(authMessage))
      assertEquals(String(success.bytes, StandardCharsets.UTF_8), s"v=${Base64.getEncoder.encodeToString(expected)}")
      assert(session.evaluate(Array.emptyByteArray).isInstanceOf[ScramFailure])
    finally Arrays.fill(password, '\u0000')
  }

  test("unknown users and invalid proofs receive the same public failure") {
    val mechanism = SaslMechanism.ScramSha256
    val password = "wrong-password".toCharArray
    try
      def fail(user: String): ScramFailure =
        val session = ScramServerSession(mechanism, _ => None, () => "SERVER")
        session.evaluate(bytes(s"n,,n=${ScramMessage.encodeName(user)},r=CLIENT"))
        val proof = Base64.getEncoder.encodeToString(new Array[Byte](32))
        session.evaluate(bytes(s"c=biws,r=CLIENTSERVER,p=$proof")).asInstanceOf[ScramFailure]

      val unknown = fail("missing")
      val other = fail("another")
      assertEquals(unknown.message, "SCRAM proof is invalid")
      assertEquals(String(unknown.bytes, StandardCharsets.UTF_8), "e=invalid-proof")
      assertEquals(String(other.bytes, StandardCharsets.UTF_8), "e=invalid-proof")
    finally Arrays.fill(password, '\u0000')
  }

  private def clientProof(
      mechanism: SaslMechanism,
      password: Array[Char],
      salt: Array[Byte],
      credential: ScramCredential,
      authMessage: String
  ): Array[Byte] =
    val specification = javax.crypto.spec.PBEKeySpec(password, salt, ScramCredential.MinimumIterations, 256)
    val salted =
      try javax.crypto.SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(specification).getEncoded
      finally specification.clearPassword()
    val clientKey = ScramCredential.hmac(mechanism, salted, bytes("Client Key"))
    val signature = ScramCredential.hmac(mechanism, credential.storedKey, bytes(authMessage))
    try clientKey.indices.map(index => (clientKey(index) ^ signature(index)).toByte).toArray
    finally
      Arrays.fill(salted, 0.toByte)
      Arrays.fill(clientKey, 0.toByte)
      Arrays.fill(signature, 0.toByte)

  private def bytes(value: String): Array[Byte] = value.getBytes(StandardCharsets.UTF_8)

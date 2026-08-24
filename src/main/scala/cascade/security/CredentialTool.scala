package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Arrays

object CredentialTool:
  def main(arguments: Array[String]): Unit =
    val (user, passwordFile, iterations) = parse(arguments.toList)
    val password = Files.readString(passwordFile, StandardCharsets.UTF_8).stripTrailing().toCharArray
    try println(generateLine(user, password, iterations))
    finally Arrays.fill(password, '\u0000')

  def generateLine(user: String, password: Array[Char], iterations: Int = CredentialHash.RecommendedIterations): String =
    require(user.nonEmpty && !user.exists(_.isWhitespace) && !user.contains('=') && !user.startsWith("#"), "invalid user name")
    s"$user=${CredentialHash.create(password, iterations)}"

  private def parse(arguments: List[String]): (String, Path, Int) = arguments match
    case user :: "--password-file" :: path :: Nil =>
      (user, Path.of(path), CredentialHash.RecommendedIterations)
    case user :: "--password-file" :: path :: "--iterations" :: iterations :: Nil =>
      (user, Path.of(path), iterations.toInt)
    case _ =>
      throw IllegalArgumentException(
        "usage: CredentialTool <user> --password-file <path> [--iterations <count>]"
      )

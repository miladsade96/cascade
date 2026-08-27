package cascade.security

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.Arrays

object CredentialTool:
  def main(arguments: Array[String]): Unit =
    val options = parse(arguments.toList)
    val password = Files.readString(options.passwordFile, StandardCharsets.UTF_8).stripTrailing().toCharArray
    try
      val line = options.mechanism match
        case SaslMechanism.Plain =>
          generateLine(options.user, password, options.iterations.getOrElse(CredentialHash.RecommendedIterations))
        case mechanism =>
          generateScramLine(options.user, password, mechanism, options.iterations.getOrElse(ScramCredential.RecommendedIterations))
      println(line)
    finally Arrays.fill(password, '\u0000')

  def generateLine(user: String, password: Array[Char], iterations: Int = CredentialHash.RecommendedIterations): String =
    require(user.nonEmpty && !user.exists(_.isWhitespace) && !user.contains('=') && !user.startsWith("#"), "invalid user name")
    s"$user=${CredentialHash.create(password, iterations)}"

  def generateScramLine(
      user: String,
      password: Array[Char],
      mechanism: SaslMechanism,
      iterations: Int = ScramCredential.RecommendedIterations
  ): String =
    val credential = ScramCredential.create(mechanism, password, iterations)
    ScramCredentialFile.encode(mechanism, user, credential)

  private final case class Options(
      user: String,
      passwordFile: Path,
      mechanism: SaslMechanism,
      iterations: Option[Int]
  )

  private def parse(arguments: List[String]): Options = arguments match
    case user :: tail if user.nonEmpty =>
      @scala.annotation.tailrec
      def loop(
          remaining: List[String],
          passwordFile: Option[Path],
          mechanism: SaslMechanism,
          iterations: Option[Int]
      ): Options = remaining match
        case Nil =>
          Options(user, passwordFile.getOrElse(usage()), mechanism, iterations)
        case "--password-file" :: path :: rest if passwordFile.isEmpty =>
          loop(rest, Some(Path.of(path)), mechanism, iterations)
        case "--mechanism" :: value :: rest =>
          loop(rest, passwordFile, SaslMechanism.parse(value), iterations)
        case "--iterations" :: value :: rest if iterations.isEmpty =>
          loop(rest, passwordFile, mechanism, Some(value.toInt))
        case _ => usage()
      loop(tail, None, SaslMechanism.Plain, None)
    case _ => usage()

  private def usage(): Nothing =
    throw IllegalArgumentException(
      "usage: CredentialTool <user> --password-file <path> [--mechanism PLAIN|SCRAM-SHA-256|SCRAM-SHA-512] [--iterations <count>]"
    )

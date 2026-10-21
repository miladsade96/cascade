package cascade.qualification

import java.io.BufferedReader
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.jdk.CollectionConverters.*

private[qualification] final case class CommandResult(exitCode: Int, output: Vector[String]):
  def text: String = output.mkString(System.lineSeparator())

private[qualification] trait CommandRunner:
  def run(command: Vector[String], timeout: Duration): CommandResult

private[qualification] object ProcessCommandRunner extends CommandRunner:
  override def run(command: Vector[String], timeout: Duration): CommandResult =
    require(command.nonEmpty, "command must not be empty")
    require(!timeout.isZero && !timeout.isNegative, "command timeout must be positive")
    val process = ProcessBuilder(command*).redirectErrorStream(true).start()
    val lines = ConcurrentLinkedQueue[String]()
    val readerThread = Thread.ofVirtual().name(s"command-${process.pid()}-output").start(() =>
      val reader = BufferedReader(process.inputReader(StandardCharsets.UTF_8))
      try
        var line = reader.readLine()
        while line != null do
          lines.add(line): Unit
          line = reader.readLine()
      finally reader.close()
    )
    val completed = process.waitFor(timeout.toMillis, TimeUnit.MILLISECONDS)
    if !completed then
      process.destroyForcibly()
      process.waitFor(10L, TimeUnit.SECONDS): Unit
    readerThread.join(10_000L)
    if !completed then
      throw IllegalStateException(s"command timed out after ${timeout.toSeconds}s: ${command.mkString(" ")}; output=${lines.asScala.mkString(" | ")}")
    CommandResult(process.exitValue(), lines.iterator().asScala.toVector)

private[qualification] final class DockerCli(
    executable: String = "docker",
    runner: CommandRunner = ProcessCommandRunner
):
  require(executable.nonEmpty, "Docker executable must not be empty")

  def execute(arguments: Seq[String], timeout: Duration = Duration.ofMinutes(2)): CommandResult =
    runner.run(executable +: arguments.toVector, timeout)

  def requireSuccess(arguments: Seq[String], timeout: Duration = Duration.ofMinutes(2)): Vector[String] =
    val result = execute(arguments, timeout)
    if result.exitCode != 0 then
      throw IllegalStateException(
        s"Docker command failed with exit ${result.exitCode}: ${arguments.mkString(" ")}; output=${result.output.mkString(" | ")}"
      )
    result.output

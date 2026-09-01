package cascade.fault

import java.io.BufferedReader
import java.net.{InetSocketAddress, Socket}
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.jdk.CollectionConverters.*

/** Forked broker JVM whose forceful termination bypasses Cascade's shutdown hook. */
final class BrokerProcess private (val process: Process, outputLines: ConcurrentLinkedQueue[String]) extends AutoCloseable:
  def pid: Long = process.pid()
  def isAlive: Boolean = process.isAlive
  def output: Vector[String] = outputLines.iterator().asScala.toVector

  def awaitListening(host: String, port: Int, timeoutMillis: Long = 15000L): Unit =
    val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
    var connected = false
    while !connected && process.isAlive && System.nanoTime() < deadline do
      val socket = Socket()
      try
        socket.connect(InetSocketAddress(host, port), 200)
        connected = true
      catch case _: Throwable => Thread.sleep(25L)
      finally socket.close()
    if !connected then
      throw IllegalStateException(s"broker process $pid did not listen on $host:$port; output=${output.mkString(" | ")}")

  def kill(): Unit =
    if process.isAlive then
      process.destroyForcibly()
      if !process.waitFor(10L, TimeUnit.SECONDS) then
        throw IllegalStateException(s"broker process $pid did not terminate")

  def stop(timeoutSeconds: Long = 30L): Unit =
    if process.isAlive then
      process.destroy()
      if !process.waitFor(timeoutSeconds, TimeUnit.SECONDS) then
        kill()

  def awaitExit(timeoutSeconds: Long): Boolean = process.waitFor(timeoutSeconds, TimeUnit.SECONDS)

  def exitCode: Option[Int] = Option.when(!process.isAlive)(process.exitValue())

  override def close(): Unit = kill()

object BrokerProcess:
  def start(arguments: Seq[String]): BrokerProcess =
    startWithClassPath(System.getProperty("java.class.path"), arguments)

  def startRuntime(runtimeDirectory: Path, arguments: Seq[String]): BrokerProcess =
    val libraryDirectory = runtimeDirectory.resolve("lib")
    require(java.nio.file.Files.isDirectory(libraryDirectory), s"runtime library directory does not exist: $libraryDirectory")
    startWithClassPath(libraryDirectory.toString + java.io.File.separator + "*", arguments)

  private def startWithClassPath(classPath: String, arguments: Seq[String]): BrokerProcess =
    val javaHome = Path.of(System.getProperty("java.home"))
    val executable = javaHome.resolve("bin").resolve(if System.getProperty("os.name").startsWith("Windows") then "java.exe" else "java")
    val command = Seq(
      executable.toString,
      "-cp",
      classPath,
      "cascade.Main"
    ) ++ arguments
    val process = ProcessBuilder(command*).redirectErrorStream(true).start()
    val lines = ConcurrentLinkedQueue[String]()
    Thread.ofVirtual().name(s"cascade-process-${process.pid()}-output").start(() =>
      val reader = BufferedReader(process.inputReader(StandardCharsets.UTF_8))
      try
        var line = reader.readLine()
        while line != null do
          lines.add(line): Unit
          line = reader.readLine()
      finally reader.close()
    )
    BrokerProcess(process, lines)

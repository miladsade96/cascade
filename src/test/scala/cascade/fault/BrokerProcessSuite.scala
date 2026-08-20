package cascade.fault

import java.net.ServerSocket
import java.nio.file.Files
import munit.FunSuite
import scala.jdk.CollectionConverters.*

final class BrokerProcessSuite extends FunSuite:
  test("a forked broker can be force-killed without a cooperative shutdown") {
    val directory = Files.createTempDirectory("cascade-force-kill")
    val port = freePort()
    val broker = BrokerProcess.start(
      Seq(
        "--host", "127.0.0.1",
        "--port", port.toString,
        "--advertised-host", "127.0.0.1",
        "--advertised-port", port.toString,
        "--data-dir", directory.toString
      )
    )
    try
      broker.awaitListening("127.0.0.1", port)
      assert(broker.isAlive)
      broker.kill()
      assert(!broker.isAlive)
    finally
      broker.close()
      deleteTree(directory)
  }

  private def freePort(): Int =
    val socket = ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()

  private def deleteTree(root: java.nio.file.Path): Unit =
    val paths = Files.walk(root)
    try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists)
    finally paths.close()

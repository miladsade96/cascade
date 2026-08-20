package cascade.fault

import cascade.broker.{BrokerConfig, KafkaBroker, RecoveryMode}
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

  test("a force-killed broker enters unclean recovery on restart") {
    val directory = Files.createTempDirectory("cascade-unclean-restart")
    val port = freePort()
    val process = BrokerProcess.start(
      Seq(
        "--host", "127.0.0.1",
        "--port", port.toString,
        "--advertised-host", "127.0.0.1",
        "--advertised-port", port.toString,
        "--data-dir", directory.toString,
        "--flush-policy", "sync"
      )
    )
    try
      process.awaitListening("127.0.0.1", port)
      process.kill()
      val restarted = KafkaBroker(BrokerConfig(bindHost = "127.0.0.1", port = 0, dataDirectory = directory))
      try
        assertEquals(restarted.recoveryMode, RecoveryMode.Unclean)
        restarted.start()
      finally restarted.close()
    finally
      process.close()
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

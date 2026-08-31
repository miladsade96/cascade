package cascade.protocol

import cascade.broker.{BrokerConfig, KafkaBroker}
import cascade.operations.OperationsConfig
import java.io.{BufferedOutputStream, DataOutputStream}
import java.net.Socket
import java.nio.file.{Files, Path}
import java.time.Duration
import java.util.{Properties, Random}
import java.util.concurrent.TimeUnit
import munit.FunSuite
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import scala.jdk.CollectionConverters.*

final class ProtocolFuzzSuite extends FunSuite:
  test("bounded random request headers fail without unbounded work") {
    val random = Random(0xCA5CADEL)
    (0 until 20_000).foreach { _ =>
      val bytes = Array.ofDim[Byte](random.nextInt(4097))
      random.nextBytes(bytes)
      try RequestHeader.decode(bytes): Unit
      catch case _: RuntimeException => ()
    }
  }

  test("a live broker survives invalid sizes, truncated frames, and deterministic random frames") {
    val directory = Files.createTempDirectory("cascade-wire-fuzz")
    val broker = KafkaBroker(
      BrokerConfig(
        bindHost = "127.0.0.1",
        port = 0,
        advertisedHost = "127.0.0.1",
        dataDirectory = directory,
        maxRequestBytes = 4096,
        replicaRecoveryChunkBytes = 4096,
        operations = OperationsConfig(logToStderr = false)
      )
    )
    try
      broker.start()
      Vector(0, -1, 4097, Int.MaxValue).foreach(size => sendPrefix(broker.boundPort, size))
      sendTruncatedFrame(broker.boundPort, declaredBytes = 1024, actualBytes = 7)

      val random = Random(0xBADC0FFEL)
      (0 until 128).foreach { _ =>
        val bytes = Array.ofDim[Byte](1 + random.nextInt(1024))
        random.nextBytes(bytes)
        sendFrame(broker.boundPort, bytes)
      }

      val properties = Properties()
      properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, broker.bootstrapServers)
      properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "10000")
      properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "5000")
      val admin = Admin.create(properties)
      try
        admin.createTopics(java.util.List.of(NewTopic("after-fuzz", 1, 1.toShort))).all().get(10, TimeUnit.SECONDS)
        assert(admin.listTopics().names().get(10, TimeUnit.SECONDS).contains("after-fuzz"))
      finally admin.close(Duration.ofSeconds(5))
      assert(broker.metricsSnapshot.running)
    finally
      broker.close()
      deleteTree(directory)
  }

  private def sendPrefix(port: Int, size: Int): Unit =
    val socket = Socket("127.0.0.1", port)
    try
      socket.setSoTimeout(1000)
      val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
      output.writeInt(size)
      output.flush()
    finally socket.close()

  private def sendTruncatedFrame(port: Int, declaredBytes: Int, actualBytes: Int): Unit =
    val socket = Socket("127.0.0.1", port)
    try
      val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
      output.writeInt(declaredBytes)
      output.write(Array.fill[Byte](actualBytes)(0x5a.toByte))
      output.flush()
    finally socket.close()

  private def sendFrame(port: Int, bytes: Array[Byte]): Unit =
    val socket = Socket("127.0.0.1", port)
    try
      val output = DataOutputStream(BufferedOutputStream(socket.getOutputStream))
      output.writeInt(bytes.length)
      output.write(bytes)
      output.flush()
    finally socket.close()

  private def deleteTree(path: Path): Unit =
    if Files.exists(path) then
      val paths = Files.walk(path)
      try paths.iterator().asScala.toVector.sortBy(_.getNameCount).reverse.foreach(Files.deleteIfExists(_): Unit)
      finally paths.close()

package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.security.{PeerSecurityConfig, PeerSecurityProtocol, PeerTlsClient, TlsConfig}
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.jdk.CollectionConverters.*

trait PeerTransport extends AutoCloseable:
  def call(node: ClusterNode, apiKey: Short, payload: Array[Byte], timeoutMillis: Int): ByteCursor

/** Persistent, ordered peer connections; failed sockets are discarded and recreated by the next call. */
final class PeerClient(
    localNodeId: Int = -1,
    security: PeerSecurityConfig = PeerSecurityConfig(),
    tls: Option[TlsConfig] = None
) extends PeerTransport:
  require(localNodeId >= -1, "local peer node ID must be -1 or non-negative")
  private val clientId = if localNodeId < 0 then "cascade-peer" else s"cascade-peer:$localNodeId"
  private val tlsClient = security.protocol match
    case PeerSecurityProtocol.Plaintext => None
    case PeerSecurityProtocol.Ssl =>
      Some(PeerTlsClient(tls.getOrElse(throw IllegalArgumentException("peer SSL requires TLS client configuration")), security))
  private val correlations = AtomicInteger(1)
  private val connections = ConcurrentHashMap[ClusterNode, PeerConnection]()
  private val closed = AtomicBoolean(false)

  override def call(node: ClusterNode, apiKey: Short, payload: Array[Byte], timeoutMillis: Int): ByteCursor =
    if closed.get() then throw IllegalStateException("peer client is closed")
    val connection = connections.computeIfAbsent(node, target => PeerConnection(target, clientId, tlsClient))
    try connection.call(apiKey, payload, timeoutMillis, correlations.getAndIncrement())
    catch
      case error: Throwable =>
        if connections.remove(node, connection) then connection.close()
        throw error

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      connections.values().asScala.foreach(_.close())
      connections.clear()

private final class PeerConnection(
    node: ClusterNode,
    clientId: String,
    tlsClient: Option[PeerTlsClient]
) extends AutoCloseable:
  private val MaximumResponseBytes = 128 * 1024 * 1024
  private var socket: Socket | Null = null
  private var input: DataInputStream | Null = null
  private var output: DataOutputStream | Null = null

  def call(apiKey: Short, payload: Array[Byte], timeoutMillis: Int, correlationId: Int): ByteCursor = synchronized {
    ensureConnected(timeoutMillis)
    val request = ByteWriter(payload.length + 32)
      .writeShort(apiKey)
      .writeShort(0)
      .writeInt(correlationId)
      .writeNullableString(Some(clientId))
      .writeBytes(payload)
      .result()
    val currentOutput = output
    val currentInput = input
    if currentOutput == null || currentInput == null then throw IllegalStateException("peer connection is not initialized")
    currentOutput.writeInt(request.length)
    currentOutput.write(request)
    currentOutput.flush()
    val size = currentInput.readInt()
    if size < 4 || size > MaximumResponseBytes then throw ProtocolException(s"invalid peer response size: $size")
    val response = new Array[Byte](size)
    currentInput.readFully(response)
    val cursor = ByteCursor(response)
    val receivedCorrelation = cursor.readInt()
    if receivedCorrelation != correlationId then
      throw ProtocolException(s"peer correlation mismatch: expected $correlationId, got $receivedCorrelation")
    cursor
  }

  override def close(): Unit = synchronized {
    Option(socket).foreach { value =>
      try value.close()
      catch case _: Throwable => ()
    }
    socket = null
    input = null
    output = null
  }

  private def ensureConnected(timeoutMillis: Int): Unit =
    if socket == null then
      val connected = tlsClient match
        case Some(client) => client.connect(node.host, node.port, timeoutMillis)
        case None =>
          val plain = Socket()
          try
            plain.connect(InetSocketAddress(node.host, node.port), timeoutMillis)
            plain.setSoTimeout(timeoutMillis)
            plain.setTcpNoDelay(true)
            plain.setKeepAlive(true)
            plain
          catch
            case error: Throwable =>
              plain.close()
              throw error
      try
        connected.setSoTimeout(timeoutMillis)
        socket = connected
        input = DataInputStream(BufferedInputStream(connected.getInputStream, 64 * 1024))
        output = DataOutputStream(BufferedOutputStream(connected.getOutputStream, 64 * 1024))
      catch
        case error: Throwable =>
          connected.close()
          throw error
    else socket.asInstanceOf[Socket].setSoTimeout(timeoutMillis)

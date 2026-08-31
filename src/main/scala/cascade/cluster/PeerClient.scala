package cascade.cluster

import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}
import cascade.security.{PeerSecurityConfig, PeerSecurityProtocol, PeerTlsClient, ReloadableTlsContext, TlsConfig}
import java.io.{BufferedInputStream, BufferedOutputStream, DataInputStream, DataOutputStream}
import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.jdk.CollectionConverters.*

trait PeerTransport extends AutoCloseable:
  def call(node: ClusterNode, apiKey: Short, payload: Array[Byte], timeoutMillis: Int): ByteCursor
  def capabilities(node: ClusterNode, timeoutMillis: Int): PeerCapabilities = PeerCapabilities.Legacy100

/** Persistent, ordered peer connections; failed sockets are discarded and recreated by the next call. */
final class PeerClient(
    localNodeId: Int = -1,
    security: PeerSecurityConfig = PeerSecurityConfig(),
    tls: Option[TlsConfig] = None,
    tlsContext: Option[ReloadableTlsContext] = None
) extends PeerTransport:
  require(localNodeId >= -1, "local peer node ID must be -1 or non-negative")
  private val clientId = if localNodeId < 0 then "cascade-peer" else s"cascade-peer:$localNodeId"
  private val tlsClient = security.protocol match
    case PeerSecurityProtocol.Plaintext => None
    case PeerSecurityProtocol.Ssl =>
      Some(
        PeerTlsClient(
          tls.getOrElse(throw IllegalArgumentException("peer SSL requires TLS client configuration")),
          security,
          tlsContext
        )
      )
  private val correlations = AtomicInteger(1)
  private val connections = ConcurrentHashMap[ClusterNode, PeerConnection]()
  private val closed = AtomicBoolean(false)

  override def call(node: ClusterNode, apiKey: Short, payload: Array[Byte], timeoutMillis: Int): ByteCursor =
    callVersion(node, apiKey, 0, payload, timeoutMillis)

  override def capabilities(node: ClusterNode, timeoutMillis: Int): PeerCapabilities =
    val response = callVersion(node, InternalApi.PeerFeatures, 0, Array.emptyByteArray, timeoutMillis)
    val error = response.readShort()
    if error != 0 then throw ProtocolException(s"peer feature negotiation failed with error $error")
    val capabilities = PeerCapabilities(
      response.readString(),
      response.readShort(),
      response.readShort(),
      response.readArray {
        response.readString() -> response.readShort()
      }.toMap
    )
    response.ensureFullyRead()
    capabilities

  private def callVersion(
      node: ClusterNode,
      apiKey: Short,
      apiVersion: Short,
      payload: Array[Byte],
      timeoutMillis: Int
  ): ByteCursor =
    if closed.get() then throw IllegalStateException("peer client is closed")
    val connection = connections.computeIfAbsent(node, target => PeerConnection(target, clientId, tlsClient))
    try connection.call(apiKey, apiVersion, payload, timeoutMillis, correlations.getAndIncrement())
    catch
      case error: Throwable =>
        if connections.remove(node, connection) then connection.close()
        throw error

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      connections.values().asScala.foreach(_.close())
      connections.clear()
      tlsClient.foreach(_.close())

private final class PeerConnection(
    node: ClusterNode,
    clientId: String,
    tlsClient: Option[PeerTlsClient]
) extends AutoCloseable:
  private val MaximumResponseBytes = 128 * 1024 * 1024
  private var socket: Socket | Null = null
  private var input: DataInputStream | Null = null
  private var output: DataOutputStream | Null = null
  private var tlsGeneration = -1L

  def call(
      apiKey: Short,
      apiVersion: Short,
      payload: Array[Byte],
      timeoutMillis: Int,
      correlationId: Int
  ): ByteCursor = synchronized {
    ensureConnected(timeoutMillis)
    val request = ByteWriter(payload.length + 32)
      .writeShort(apiKey)
      .writeShort(apiVersion)
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
    tlsGeneration = -1L
  }

  private def ensureConnected(timeoutMillis: Int): Unit =
    if socket != null && tlsClient.exists(_.generation != tlsGeneration) then close()
    if socket == null then
      val (connected, connectedGeneration) = tlsClient match
        case Some(client) =>
          val current = client.connectCurrent(node.host, node.port, timeoutMillis)
          (current.socket, current.generation)
        case None =>
          val plain = Socket()
          try
            plain.connect(InetSocketAddress(node.host, node.port), timeoutMillis)
            plain.setSoTimeout(timeoutMillis)
            plain.setTcpNoDelay(true)
            plain.setKeepAlive(true)
            (plain, -1L)
          catch
            case error: Throwable =>
              plain.close()
              throw error
      try
        connected.setSoTimeout(timeoutMillis)
        socket = connected
        tlsGeneration = connectedGeneration
        input = DataInputStream(BufferedInputStream(connected.getInputStream, 64 * 1024))
        output = DataOutputStream(BufferedOutputStream(connected.getOutputStream, 64 * 1024))
      catch
        case error: Throwable =>
          connected.close()
          throw error
    else socket.asInstanceOf[Socket].setSoTimeout(timeoutMillis)

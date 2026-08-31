package cascade.security

import java.net.InetSocketAddress
import javax.net.ssl.SSLSocket

final case class PeerTlsConnection(socket: SSLSocket, generation: Long)

final class PeerTlsClient(
    tls: TlsConfig,
    peer: PeerSecurityConfig,
    sharedContext: Option[ReloadableTlsContext] = None
) extends AutoCloseable:
  require(peer.protocol == PeerSecurityProtocol.Ssl, "peer TLS client requires the SSL peer protocol")
  private val context = sharedContext.getOrElse(ReloadableTlsContext(tls))
  private val ownsContext = sharedContext.isEmpty

  def generation: Long = context.current.generation

  def connect(host: String, port: Int, timeoutMillis: Int): SSLSocket =
    connectCurrent(host, port, timeoutMillis).socket

  def connectCurrent(host: String, port: Int, timeoutMillis: Int): PeerTlsConnection =
    val current = context.current
    val socket = current.context.getSocketFactory.createSocket().asInstanceOf[SSLSocket]
    try
      socket.setEnabledProtocols(tls.enabledProtocols.toArray)
      val parameters = socket.getSSLParameters
      parameters.setEndpointIdentificationAlgorithm(peer.endpointIdentificationAlgorithm)
      socket.setSSLParameters(parameters)
      socket.connect(InetSocketAddress(host, port), timeoutMillis)
      socket.setSoTimeout(timeoutMillis)
      socket.setTcpNoDelay(true)
      socket.setKeepAlive(true)
      socket.startHandshake()
      PeerTlsConnection(socket, current.generation)
    catch
      case error: Throwable =>
        socket.close()
        throw error

  override def close(): Unit = if ownsContext then context.close()

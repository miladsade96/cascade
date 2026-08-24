package cascade.security

import java.net.InetSocketAddress
import javax.net.ssl.SSLSocket

final class PeerTlsClient(tls: TlsConfig, peer: PeerSecurityConfig):
  require(peer.protocol == PeerSecurityProtocol.Ssl, "peer TLS client requires the SSL peer protocol")
  private val context = TlsContextFactory.create(tls)

  def connect(host: String, port: Int, timeoutMillis: Int): SSLSocket =
    val socket = context.getSocketFactory.createSocket().asInstanceOf[SSLSocket]
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
      socket
    catch
      case error: Throwable =>
        socket.close()
        throw error


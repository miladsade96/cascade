package cascade.fault

import cascade.cluster.*
import cascade.protocol.ByteCursor
import munit.FunSuite

final class FaultCapabilitySuite extends FunSuite:
  test("capabilities cross the fault transport and respect directional blocks") {
    val faults = NetworkFaultController(2)
    val delegate = new PeerTransport:
      override def call(node: ClusterNode, api: Short, payload: Array[Byte], timeout: Int): ByteCursor = ByteCursor(payload)
      override def capabilities(node: ClusterNode, timeout: Int): PeerCapabilities = PeerCapabilities.Current
      override def close(): Unit = ()
    val transport = FaultInjectingPeerTransport(1, faults, delegate)
    val node = ClusterNode(2, "localhost", 9092)
    assertEquals(transport.capabilities(node, 100), PeerCapabilities.Current)
    faults.block(FaultSelector(1, 2, Some(InternalApi.PeerFeatures)))
    intercept[java.net.SocketTimeoutException](transport.capabilities(node, 100))
    intercept[java.net.SocketTimeoutException](transport.capabilities(node, 100))
    assertEquals(faults.calls.size, 2)
  }

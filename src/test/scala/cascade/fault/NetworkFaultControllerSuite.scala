package cascade.fault

import cascade.cluster.{ClusterNode, PeerTransport}
import cascade.protocol.ByteCursor
import java.net.SocketTimeoutException
import munit.FunSuite

final class NetworkFaultControllerSuite extends FunSuite:
  test("directional and API-specific faults only reject matching calls") {
    val faults = NetworkFaultController()
    val metadata = FaultSelector(1, 2, Some((-101).toShort))
    faults.block(metadata)

    intercept[SocketTimeoutException](faults.beforeCall(PeerCall(1, 2, -101, Vector.empty)))
    faults.beforeCall(PeerCall(1, 2, -106, Vector.empty))
    faults.beforeCall(PeerCall(2, 1, -101, Vector.empty))
    assertEquals(faults.calls.size, 3)

    faults.unblock(metadata)
    faults.beforeCall(PeerCall(1, 2, -101, Vector.empty))
  }

  test("a partition is bidirectional and healing restores every link") {
    val faults = NetworkFaultController()
    faults.partition(Set(1, 2), Set(3, 4))

    intercept[SocketTimeoutException](faults.beforeCall(PeerCall(1, 3, -106, Vector.empty)))
    intercept[SocketTimeoutException](faults.beforeCall(PeerCall(4, 2, -106, Vector.empty)))
    faults.beforeCall(PeerCall(1, 2, -106, Vector.empty))

    faults.heal()
    faults.beforeCall(PeerCall(1, 3, -106, Vector.empty))
    faults.beforeCall(PeerCall(4, 2, -106, Vector.empty))
  }

  test("the injecting transport drops before delegation and delegates after healing") {
    val faults = NetworkFaultController()
    val target = ClusterNode(2, "127.0.0.1", 9093)
    var calls = 0
    var closed = false
    val delegate = new PeerTransport:
      override def call(node: ClusterNode, apiKey: Short, payload: Array[Byte], timeoutMillis: Int): ByteCursor =
        calls += 1
        ByteCursor(Array.emptyByteArray)
      override def close(): Unit = closed = true
    val transport = FaultInjectingPeerTransport(1, faults, delegate)

    faults.block(FaultSelector(1, 2))
    intercept[SocketTimeoutException](transport.call(target, -106, Array[Byte](1), 100))
    assertEquals(calls, 0)

    faults.heal()
    transport.call(target, -106, Array[Byte](2), 100)
    assertEquals(calls, 1)
    assertEquals(faults.calls.map(_.payload), Vector(Vector[Byte](1), Vector[Byte](2)))
    transport.close()
    assert(closed)
  }

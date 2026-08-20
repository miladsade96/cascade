package cascade.fault

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

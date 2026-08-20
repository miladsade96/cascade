package cascade.fault

import cascade.cluster.{ClusterNode, PeerTransport}
import cascade.protocol.ByteCursor

final class FaultInjectingPeerTransport(
    localNodeId: Int,
    faults: NetworkFaultController,
    delegate: PeerTransport
) extends PeerTransport:
  override def call(node: ClusterNode, apiKey: Short, payload: Array[Byte], timeoutMillis: Int): ByteCursor =
    faults.beforeCall(PeerCall(localNodeId, node.id, apiKey, payload.toVector))
    delegate.call(node, apiKey, payload, timeoutMillis)

  override def close(): Unit = delegate.close()

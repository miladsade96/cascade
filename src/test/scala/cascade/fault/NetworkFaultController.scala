package cascade.fault

import java.net.SocketTimeoutException

final case class PeerCall(sourceId: Int, targetId: Int, apiKey: Short, payload: Vector[Byte])

final case class FaultSelector(sourceId: Int, targetId: Int, apiKey: Option[Short] = None):
  def matches(call: PeerCall): Boolean =
    sourceId == call.sourceId && targetId == call.targetId && apiKey.forall(_ == call.apiKey)

/** Thread-safe deterministic link control used by the cluster fault-qualification suites. */
final class NetworkFaultController:
  private var blocked = Set.empty[FaultSelector]
  private var observed = Vector.empty[PeerCall]

  def block(selector: FaultSelector): Unit = synchronized {
    blocked += selector
  }

  def unblock(selector: FaultSelector): Unit = synchronized {
    blocked -= selector
  }

  def partition(first: Set[Int], second: Set[Int]): Unit = synchronized {
    for
      source <- first
      target <- second
    do
      blocked += FaultSelector(source, target)
      blocked += FaultSelector(target, source)
  }

  def heal(): Unit = synchronized {
    blocked = Set.empty
  }

  def calls: Vector[PeerCall] = synchronized(observed)

  private[fault] def beforeCall(call: PeerCall): Unit = synchronized {
    observed :+= call
    if blocked.exists(_.matches(call)) then
      throw SocketTimeoutException(
        s"injected peer partition ${call.sourceId}->${call.targetId} api=${call.apiKey}"
      )
  }

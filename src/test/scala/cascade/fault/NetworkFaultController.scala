package cascade.fault

import java.net.SocketTimeoutException

final case class PeerCall(sourceId: Int, targetId: Int, apiKey: Short, payload: Vector[Byte])

final case class FaultSelector(sourceId: Int, targetId: Int, apiKey: Option[Short] = None):
  def matches(call: PeerCall): Boolean =
    sourceId == call.sourceId && targetId == call.targetId && apiKey.forall(_ == call.apiKey)

final class ArmedFault(
    triggerMatches: Int,
    trigger: PeerCall => Boolean,
    drop: PeerCall => Boolean
):
  require(triggerMatches > 0, "trigger match count must be positive")
  private var matches = 0
  private var armed = false

  def isArmed: Boolean = armed

  private[fault] def evaluate(call: PeerCall): Boolean =
    if armed then drop(call)
    else
      if trigger(call) then
        matches += 1
        armed = matches >= triggerMatches
      false

/** Thread-safe deterministic link control used by the cluster fault-qualification suites. */
final class NetworkFaultController(maxRecordedCalls: Int = 10000):
  require(maxRecordedCalls >= 0, "recorded call limit must be non-negative")
  private var blocked = Set.empty[FaultSelector]
  private var observed = Vector.empty[PeerCall]
  private var armedFaults = Vector.empty[ArmedFault]

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

  def arm(fault: ArmedFault): Unit = synchronized {
    armedFaults :+= fault
  }

  def heal(): Unit = synchronized {
    blocked = Set.empty
    armedFaults = Vector.empty
  }

  def calls: Vector[PeerCall] = synchronized(observed)

  private[fault] def beforeCall(call: PeerCall): Unit = synchronized {
    if maxRecordedCalls > 0 then observed = (observed :+ call).takeRight(maxRecordedCalls)
    if blocked.exists(_.matches(call)) || armedFaults.exists(_.evaluate(call)) then
      throw SocketTimeoutException(
        s"injected peer partition ${call.sourceId}->${call.targetId} api=${call.apiKey}"
      )
  }

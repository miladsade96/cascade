package cascade.fault

import java.net.SocketTimeoutException
import java.util.concurrent.{CountDownLatch, TimeUnit}
import cascade.protocol.ByteCursor

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

final case class PeerPause(
    selector: FaultSelector,
    entered: CountDownLatch,
    release: CountDownLatch,
    timeoutMillis: Long
):
  require(timeoutMillis > 0L, "pause timeout must be positive")

/** Thread-safe deterministic link control used by the cluster fault-qualification suites. */
final class NetworkFaultController(maxRecordedCalls: Int = 10000):
  require(maxRecordedCalls >= 0, "recorded call limit must be non-negative")
  private var blocked = Set.empty[FaultSelector]
  private var observed = Vector.empty[PeerCall]
  private var armedFaults = Vector.empty[ArmedFault]
  private var pauses = Vector.empty[PeerPause]
  @volatile private var replyObserver: Option[(PeerCall, Array[Byte]) => Unit] = None

  /** Observe or pause a completed RPC after its connection lock has been released. */
  def observeReplies(observer: (PeerCall, Array[Byte]) => Unit): Unit =
    replyObserver = Some(observer)

  private[fault] def afterCall(call: PeerCall, response: ByteCursor): ByteCursor =
    replyObserver match
      case None => response
      case Some(observer) =>
        val bytes = response.readBytes(response.remaining)
        observer(call, bytes)
        ByteCursor(bytes)

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

  def pause(value: PeerPause): Unit = synchronized {
    pauses :+= value
  }

  def resume(selector: FaultSelector): Unit = synchronized {
    pauses.filter(_.selector == selector).foreach(_.release.countDown())
    pauses = pauses.filterNot(_.selector == selector)
  }

  def heal(): Unit = synchronized {
    blocked = Set.empty
    armedFaults = Vector.empty
    pauses.foreach(_.release.countDown())
    pauses = Vector.empty
    replyObserver = None
  }

  def calls: Vector[PeerCall] = synchronized(observed)

  private[fault] def beforeCall(call: PeerCall): Unit =
    val pause = synchronized {
      if maxRecordedCalls > 0 then observed = (observed :+ call).takeRight(maxRecordedCalls)
      if blocked.exists(_.matches(call)) || armedFaults.exists(_.evaluate(call)) then
        throw SocketTimeoutException(
          s"injected peer partition ${call.sourceId}->${call.targetId} api=${call.apiKey}"
        )
      pauses.find(_.selector.matches(call))
    }
    pause.foreach { value =>
      value.entered.countDown()
      if !value.release.await(value.timeoutMillis, TimeUnit.MILLISECONDS) then
        throw SocketTimeoutException(
          s"injected peer pause timed out ${call.sourceId}->${call.targetId} api=${call.apiKey}"
        )
    }

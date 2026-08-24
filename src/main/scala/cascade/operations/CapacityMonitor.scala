package cascade.operations

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.util.control.NonFatal

final class CapacityMonitor(
    config: CapacityAlertConfig,
    snapshot: () => BrokerMetricsSnapshot,
    limits: CapacityLimits,
    onRaised: CapacityAlert => Unit,
    onResolved: String => Unit,
    onError: Throwable => Unit = _ => (),
    clockMillis: () => Long = () => System.currentTimeMillis()
) extends AutoCloseable:
  private val executor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-capacity-monitor").factory())
  private val active = AtomicReference(Map.empty[String, Long])
  private val started = AtomicBoolean(false)
  private val closed = AtomicBoolean(false)

  def start(): Unit =
    if closed.get() then throw IllegalStateException("capacity monitor is closed")
    if !started.compareAndSet(false, true) then throw IllegalStateException("capacity monitor is already running")
    executor.scheduleAtFixedRate(
      () => safePoll(),
      0L,
      config.intervalMillis,
      TimeUnit.MILLISECONDS
    ): Unit

  def activeAlerts: Set[String] = active.get().keySet

  private[operations] def poll(): Unit = synchronized {
    val now = clockMillis()
    val evaluated = CapacityAlerts.evaluate(snapshot(), limits, config)
    val current = evaluated.iterator.map(alert => alert.code -> alert).toMap
    val previous = active.get()
    val next = scala.collection.mutable.Map.from(previous)

    evaluated.foreach { alert =>
      if previous.get(alert.code).forall(last => now - last >= config.repeatIntervalMillis) then
        onRaised(alert)
        next.update(alert.code, now)
    }
    previous.keySet.diff(current.keySet).foreach { code =>
      onResolved(code)
      next.remove(code): Unit
    }
    active.set(next.toMap)
  }

  override def close(): Unit =
    if closed.compareAndSet(false, true) then executor.shutdownNow(): Unit

  private def safePoll(): Unit =
    try poll()
    catch case NonFatal(error) => onError(error)


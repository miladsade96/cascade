package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.util.control.NonFatal

/** One worker and at most one pending image, regardless of publication rate. */
final class CoordinatorImageInstaller(install: CoordinatorMetadata => Unit) extends AutoCloseable:
  private val pending = AtomicReference[CoordinatorMetadata](null)
  private val ready = Semaphore(0)
  private val closed = AtomicBoolean(false)
  private val worker = Thread.ofVirtual().name("cascade-coordinator-installer").start(() => run())

  def offer(image: CoordinatorMetadata): Unit =
    if !closed.get() then
      val previous = pending.getAndUpdate { existing =>
        if existing == null || image.version > existing.version then image else existing
      }
      if previous == null then ready.release()

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      worker.interrupt()
      worker.join(5000L)
      pending.set(null)

  private def run(): Unit =
    while !closed.get() do
      try
        ready.acquire()
        val image = pending.getAndSet(null)
        if image != null && !closed.get() then install(image)
      catch
        case _: InterruptedException => ()
        case NonFatal(error) => System.err.println(s"Cascade coordinator installation failed: ${error.getMessage}")

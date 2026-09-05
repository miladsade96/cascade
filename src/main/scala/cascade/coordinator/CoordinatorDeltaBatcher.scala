package cascade.coordinator

import cascade.protocol.Errors
import java.util.concurrent.{CompletableFuture, TimeUnit, TimeoutException}
import scala.collection.mutable
import scala.util.control.NonFatal

/** Bounded controller-local queue that combines compatible shard proposals into one quorum publication. */
final class CoordinatorDeltaBatcher(
    config: CoordinatorPublicationConfig,
    publish: Vector[CoordinatorDelta] => Vector[Short]
) extends AutoCloseable:
  private final class Entry(val delta: CoordinatorDelta):
    val bytes: Long = CoordinatorDeltaCodec.encode(delta).length.toLong
    val enqueuedNanos: Long = System.nanoTime()
    val result: CompletableFuture[Short] = CompletableFuture[Short]()
    var claimed = false
    var publishing = false
    var cancelled = false

  private val monitor = Object()
  private val queue = mutable.ArrayDeque.empty[Entry]
  private var pendingRequests = 0
  private var pendingBytes = 0L
  private var peakRequests = 0
  private var peakBytes = 0L
  private var accepted = 0L
  private var rejected = 0L
  private var completed = 0L
  private var failed = 0L
  private var batches = 0L
  private var batchRequests = 0L
  private var committedBatches = 0L
  private var committedRequests = 0L
  private var conflictedRequests = 0L
  private var queueNanos = 0L
  @volatile private var closed = false
  private val worker = Thread.ofPlatform().daemon().name("cascade-coordinator-publisher").start(() => run())

  def snapshot: CoordinatorPublicationSnapshot = monitor.synchronized {
    CoordinatorPublicationSnapshot(pendingRequests, pendingBytes, peakRequests, peakBytes, accepted, rejected,
      completed, failed, batches, batchRequests, committedBatches, committedRequests, conflictedRequests, queueNanos)
  }

  def submit(delta: CoordinatorDelta): Short =
    val entry = Entry(delta)
    val admitted = monitor.synchronized {
      if closed then false
      else if entry.bytes > config.maxBytes || pendingRequests >= config.maxPendingRequests ||
          entry.bytes > config.maxPendingBytes - pendingBytes then false
      else
        queue.append(entry)
        pendingRequests += 1
        pendingBytes += entry.bytes
        peakRequests = math.max(peakRequests, pendingRequests)
        peakBytes = math.max(peakBytes, pendingBytes)
        accepted += 1L
        monitor.notifyAll()
        true
    }
    if !admitted then
      monitor.synchronized(rejected += 1L)
      if closed then Errors.CoordinatorNotAvailable
      else if entry.bytes > config.maxBytes then Errors.InvalidRequest
      else Errors.RequestTimedOut
    else await(entry)

  private def await(entry: Entry): Short =
    var interrupted = false
    try
      while !entry.result.isDone do
        try
          val remaining = config.queueTimeoutMillis * 1_000_000L - (System.nanoTime() - entry.enqueuedNanos)
          if remaining > 0L then entry.result.get(remaining, TimeUnit.NANOSECONDS): Unit
          else if !cancel(entry, Errors.RequestTimedOut) then entry.result.get(): Unit
        catch
          case _: TimeoutException => cancel(entry, Errors.RequestTimedOut): Unit
          case _: InterruptedException =>
            interrupted = true
            cancel(entry, Errors.CoordinatorNotAvailable): Unit
      entry.result.join()
    finally if interrupted then Thread.currentThread().interrupt()

  private def cancel(entry: Entry, error: Short): Boolean = monitor.synchronized {
    if entry.publishing then false
    else
      if !entry.cancelled && !entry.result.isDone then
        entry.cancelled = true
        if !entry.claimed then
          queue.filterInPlace(_ ne entry)
          release(entry)
        complete(entry, error)
        monitor.notifyAll()
      true
  }

  private def takeBatch(): Vector[Entry] = monitor.synchronized {
    while queue.isEmpty && !closed do monitor.wait()
    if closed then return Vector.empty
    val oldest = queue.head.enqueuedNanos
    var remaining = config.lingerMillis * 1_000_000L - (System.nanoTime() - oldest)
    while !closed && queue.nonEmpty && queue.size < config.maxRequests && remaining > 0L do
      TimeUnit.NANOSECONDS.timedWait(monitor, remaining)
      remaining = config.lingerMillis * 1_000_000L - (System.nanoTime() - oldest)
    if closed || queue.isEmpty then return Vector.empty
    val selected = Vector.newBuilder[Entry]
    var count = 0
    var bytes = 0L
    while queue.nonEmpty && count < config.maxRequests && queue.head.bytes <= config.maxBytes - bytes do
      val entry = queue.removeHead()
      entry.claimed = true
      entry.publishing = true
      queueNanos += math.max(0L, System.nanoTime() - entry.enqueuedNanos)
      selected += entry
      count += 1
      bytes += entry.bytes
    batches += 1L
    batchRequests += count
    selected.result()
  }

  private def run(): Unit =
    try
      while !closed do
        val batch = takeBatch()
        if batch.nonEmpty then
          try
            val results = publish(batch.map(_.delta))
            require(results.size == batch.size, "coordinator publication result count mismatch")
            monitor.synchronized {
              val successful = results.count(_ == Errors.None)
              if successful > 0 then committedBatches += 1L
              committedRequests += successful.toLong
              conflictedRequests += results.count(_ == Errors.CoordinatorLoadInProgress).toLong
            }
            batch.zip(results).foreach((entry, code) => complete(entry, code))
          catch case NonFatal(_) => batch.foreach(entry => complete(entry, Errors.CoordinatorNotAvailable))
          finally monitor.synchronized {
            batch.foreach { entry =>
              complete(entry, Errors.CoordinatorNotAvailable)
              release(entry)
            }
          }
    finally stopPending()

  private def release(entry: Entry): Unit =
    pendingRequests -= 1
    pendingBytes -= entry.bytes

  private def complete(entry: Entry, code: Short): Unit = monitor.synchronized {
    if !entry.result.isDone then
      completed += 1L
      if code != Errors.None then failed += 1L
      entry.result.complete(code): Unit
  }

  private def stopPending(): Unit = monitor.synchronized {
    closed = true
    queue.foreach { entry =>
      entry.cancelled = true
      complete(entry, Errors.CoordinatorNotAvailable)
      release(entry)
    }
    queue.clear()
    monitor.notifyAll()
  }

  override def close(): Unit =
    stopPending()
    if Thread.currentThread() ne worker then
      var interrupted = false
      try
        while worker.isAlive do
          try worker.join()
          catch case _: InterruptedException => interrupted = true
      finally if interrupted then Thread.currentThread().interrupt()

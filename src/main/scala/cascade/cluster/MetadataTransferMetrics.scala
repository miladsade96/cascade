package cascade.cluster

import java.util.concurrent.atomic.AtomicLong

final case class MetadataTransferSnapshot(deltaBytes: Long = 0L, fullBytes: Long = 0L, fallbacks: Long = 0L)

object MetadataTransferSnapshot:
  val Empty: MetadataTransferSnapshot = MetadataTransferSnapshot()

/** Attempted outbound RPC body bytes, excluding Kafka framing, TCP, and TLS overhead. */
final class MetadataTransferMetrics:
  private val deltaBytes = AtomicLong(0L)
  private val fullBytes = AtomicLong(0L)
  private val fallbacks = AtomicLong(0L)

  def recordDelta(bytes: Int): Unit = deltaBytes.addAndGet(bytes.toLong): Unit
  def recordFull(bytes: Int): Unit = fullBytes.addAndGet(bytes.toLong): Unit
  def recordFallback(): Unit = fallbacks.incrementAndGet(): Unit
  def snapshot: MetadataTransferSnapshot = MetadataTransferSnapshot(deltaBytes.get(), fullBytes.get(), fallbacks.get())

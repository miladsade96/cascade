package cascade.security

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

enum QuotaDecision:
  case Allowed
  case Throttle(delayMillis: Long)
  case Rejected(requiredDelayMillis: Long)

final case class RequestQuotaSnapshot(throttled: Long, rejected: Long, throttleMillis: Long, principals: Int)

object RequestQuotaSnapshot:
  val Empty: RequestQuotaSnapshot = RequestQuotaSnapshot(0L, 0L, 0L, 0)

final class RequestQuota(
    bytesPerSecond: Long,
    configuredBurstBytes: Long,
    maxThrottleMillis: Long,
    nanoTime: () => Long = () => System.nanoTime()
):
  require(bytesPerSecond >= 0L, "request quota cannot be negative")
  require(configuredBurstBytes >= 0L, "request quota burst cannot be negative")
  require(maxThrottleMillis >= 0L, "maximum throttle cannot be negative")

  private val burstBytes = if configuredBurstBytes > 0L then configuredBurstBytes else bytesPerSecond
  private val buckets = ConcurrentHashMap[String, TokenBucket]()
  private val throttled = AtomicLong(0L)
  private val rejected = AtomicLong(0L)
  private val totalThrottleMillis = AtomicLong(0L)

  def evaluate(principal: String, bytes: Int, rejectExcess: Boolean = true): QuotaDecision =
    if bytesPerSecond == 0L || bytes <= 0 then QuotaDecision.Allowed
    else
      val bucket = buckets.computeIfAbsent(principal, _ => TokenBucket(bytesPerSecond, burstBytes, nanoTime))
      bucket.reserve(bytes.toLong, maxThrottleMillis, rejectExcess) match
        case decision @ QuotaDecision.Throttle(delay) =>
          throttled.incrementAndGet(): Unit
          totalThrottleMillis.addAndGet(delay): Unit
          decision
        case decision @ QuotaDecision.Rejected(_) =>
          rejected.incrementAndGet(): Unit
          decision
        case QuotaDecision.Allowed => QuotaDecision.Allowed

  def snapshot: RequestQuotaSnapshot =
    RequestQuotaSnapshot(throttled.get(), rejected.get(), totalThrottleMillis.get(), buckets.size())

private final class TokenBucket(rate: Long, burst: Long, nanoTime: () => Long):
  private var tokens = burst.toDouble
  private var lastRefillNanos = nanoTime()

  def reserve(bytes: Long, maxThrottleMillis: Long, rejectExcess: Boolean): QuotaDecision = synchronized {
    val now = nanoTime()
    val elapsed = math.max(0L, now - lastRefillNanos)
    tokens = math.min(burst.toDouble, tokens + elapsed.toDouble * rate.toDouble / 1_000_000_000d)
    lastRefillNanos = now
    tokens -= bytes.toDouble
    if tokens >= 0d then QuotaDecision.Allowed
    else
      val requiredMillis = math.max(1L, math.ceil((-tokens * 1000d) / rate.toDouble).toLong)
      if requiredMillis > maxThrottleMillis && rejectExcess then
        tokens += bytes.toDouble
        QuotaDecision.Rejected(requiredMillis)
      else QuotaDecision.Throttle(math.min(requiredMillis, maxThrottleMillis))
  }

package cascade.security

import cascade.protocol.Errors
import scala.collection.mutable

enum QuotaKind(val id: Byte):
  case Request extends QuotaKind(0)
  case Response extends QuotaKind(1)
  case Produce extends QuotaKind(2)
  case Fetch extends QuotaKind(3)

object QuotaKind:
  val All: Vector[QuotaKind] = Vector(Request, Response, Produce, Fetch)

  def fromId(id: Byte): Option[QuotaKind] = All.find(_.id == id)

final case class QuotaLimit(bytesPerSecond: Long, burstBytes: Long, maxThrottleMillis: Long):
  require(bytesPerSecond >= 0L, "quota rate cannot be negative")
  require(burstBytes >= 0L, "quota burst cannot be negative")
  require(maxThrottleMillis >= 0L, "maximum throttle cannot be negative")

  def effectiveBurstBytes: Long = if burstBytes > 0L then burstBytes else bytesPerSecond

object QuotaLimit:
  def forKind(resources: ResourceLimits, kind: QuotaKind): QuotaLimit = kind match
    case QuotaKind.Request =>
      QuotaLimit(resources.requestBytesPerSecond, resources.requestBurstBytes, resources.maxThrottleMillis)
    case QuotaKind.Response =>
      QuotaLimit(resources.responseBytesPerSecond, resources.responseBurstBytes, resources.maxThrottleMillis)
    case QuotaKind.Produce =>
      QuotaLimit(resources.produceBytesPerSecond, resources.produceBurstBytes, resources.maxThrottleMillis)
    case QuotaKind.Fetch =>
      QuotaLimit(resources.fetchBytesPerSecond, resources.fetchBurstBytes, resources.maxThrottleMillis)

final case class ClusterQuotaReservation(
    kind: QuotaKind,
    principal: String,
    bytes: Int,
    rejectExcess: Boolean,
    limit: QuotaLimit,
    controllerTerm: Long
):
  require(principal.nonEmpty, "quota principal cannot be empty")
  require(bytes > 0, "quota reservation bytes must be positive")
  require(controllerTerm >= 0L, "quota controller term cannot be negative")

final case class ClusterQuotaResult(errorCode: Short, decision: Option[QuotaDecision], controllerTerm: Long)

final case class DistributedQuotaSnapshot(
    controllerTerm: Long = -1L,
    principals: Int = 0,
    reservations: Long = 0L,
    allowed: Long = 0L,
    throttled: Long = 0L,
    rejected: Long = 0L,
    configurationMismatches: Long = 0L,
    epochResets: Long = 0L,
    forwarded: Long = 0L,
    failures: Long = 0L
)

/** One active controller owns every cluster-wide principal bucket for its fenced controller term. */
final class ClusterQuotaLedger(resources: ResourceLimits, nanoTime: () => Long = () => System.nanoTime()):
  private val buckets = mutable.HashMap.empty[(QuotaKind, String), TokenBucket]
  private var activeTerm = -1L
  private var reservations = 0L
  private var allowed = 0L
  private var throttled = 0L
  private var rejected = 0L
  private var configurationMismatches = 0L
  private var epochResets = 0L
  private var forwarded = 0L
  private var failures = 0L

  def reserve(request: ClusterQuotaReservation): ClusterQuotaResult = synchronized {
    if request.controllerTerm < activeTerm then
      ClusterQuotaResult(Errors.NotController, None, activeTerm)
    else
      if request.controllerTerm > activeTerm then
        buckets.clear()
        activeTerm = request.controllerTerm
        epochResets += 1L
      val expected = QuotaLimit.forKind(resources, request.kind)
      if request.limit != expected then
        configurationMismatches += 1L
        ClusterQuotaResult(Errors.InvalidConfiguration, None, activeTerm)
      else if expected.bytesPerSecond == 0L then
        ClusterQuotaResult(Errors.None, Some(QuotaDecision.Allowed), activeTerm)
      else
        reservations += 1L
        val bucket = buckets.getOrElseUpdate((request.kind, request.principal), TokenBucket(nanoTime, startFull = false))
        val decision = bucket.reserve(
          request.bytes.toLong,
          expected.bytesPerSecond.toDouble,
          expected.effectiveBurstBytes.toDouble,
          expected.maxThrottleMillis,
          request.rejectExcess
        )
        decision match
          case QuotaDecision.Allowed      => allowed += 1L
          case QuotaDecision.Throttle(_)  => throttled += 1L
          case QuotaDecision.Rejected(_)  => rejected += 1L
        ClusterQuotaResult(Errors.None, Some(decision), activeTerm)
  }

  def recordForwarded(): Unit = synchronized { forwarded += 1L }

  def recordFailure(): Unit = synchronized { failures += 1L }

  def snapshot: DistributedQuotaSnapshot = synchronized {
    DistributedQuotaSnapshot(
      controllerTerm = activeTerm,
      principals = buckets.keysIterator.map(_._2).toSet.size,
      reservations = reservations,
      allowed = allowed,
      throttled = throttled,
      rejected = rejected,
      configurationMismatches = configurationMismatches,
      epochResets = epochResets,
      forwarded = forwarded,
      failures = failures
    )
  }

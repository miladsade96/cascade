package cascade.security

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

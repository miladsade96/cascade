package cascade.operations

import cascade.security.SaslMechanism
import java.util.concurrent.atomic.AtomicLongArray

final case class MechanismAuthenticationSnapshot(mechanism: String, successes: Long, failures: Long)

final case class AuthenticationSnapshot(mechanisms: Vector[MechanismAuthenticationSnapshot]):
  def successes: Long = mechanisms.map(_.successes).sum
  def failures: Long = mechanisms.map(_.failures).sum

object AuthenticationSnapshot:
  private val Names = SaslMechanism.Supported.map(_.wireName) :+ "UNKNOWN"
  val Empty: AuthenticationSnapshot = AuthenticationSnapshot(
    Names.map(MechanismAuthenticationSnapshot(_, 0L, 0L))
  )

final class AuthenticationMetrics:
  private val names = SaslMechanism.Supported.map(_.wireName) :+ "UNKNOWN"
  private val successes = AtomicLongArray(names.size)
  private val failures = AtomicLongArray(names.size)

  def recordSuccess(mechanism: Option[String]): Unit = successes.incrementAndGet(index(mechanism)): Unit

  def recordFailure(mechanism: Option[String]): Unit = failures.incrementAndGet(index(mechanism)): Unit

  def snapshot: AuthenticationSnapshot = AuthenticationSnapshot(
    names.indices.map { index =>
      MechanismAuthenticationSnapshot(names(index), successes.get(index), failures.get(index))
    }.toVector
  )

  private def index(mechanism: Option[String]): Int =
    mechanism.flatMap(value => names.indices.find(index => names(index) == value)).getOrElse(names.size - 1)

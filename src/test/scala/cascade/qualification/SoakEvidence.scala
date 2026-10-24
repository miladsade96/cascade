package cascade.qualification

import java.nio.charset.StandardCharsets
import java.nio.file.{AtomicMoveNotSupportedException, Files, Path, StandardCopyOption}
import java.time.Instant
import java.util.Locale
import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters.*

enum SoakProfile(val cliName: String):
  case Local extends SoakProfile("local")
  case AuthenticatedCluster extends SoakProfile("authenticated-cluster")

object SoakProfile:
  def parse(value: String): SoakProfile =
    SoakProfile.values.find(_.cliName == value.trim.toLowerCase).getOrElse {
      throw IllegalArgumentException(s"unsupported soak profile: $value")
    }

final case class SoakCounters(
    records: Long = 0L,
    verifiedRecords: Long = 0L,
    recoveredRecords: Long = 0L,
    abortedRecords: Long = 0L,
    cycles: Long = 0L,
    mismatches: Long = 0L,
    brokerRestarts: Long = 0L,
    controllerFailovers: Long = 0L,
    rebalances: Long = 0L,
    tlsRotations: Long = 0L,
    quotaReservations: Long = 0L,
    quotaThrottled: Long = 0L,
    quotaRejected: Long = 0L,
    authenticationSuccesses: Long = 0L,
    authenticationFailures: Long = 0L,
    authenticatedTenants: Int = 0,
    peakHeapBytes: Long = 0L,
    restartVerified: Boolean = false
)

final case class SoakReport(
    profile: SoakProfile,
    startedAt: Instant,
    completedAt: Instant,
    configuredDurationSeconds: Long,
    elapsedSeconds: Double,
    tenants: Int,
    revision: String,
    counters: SoakCounters,
    failure: Option[String] = None
):
  def records: Long = counters.records
  def cycles: Long = counters.cycles
  def peakHeapBytes: Long = counters.peakHeapBytes
  def mismatches: Long = counters.mismatches

  def passed: Boolean =
    failure.isEmpty && counters.records > 0L && counters.verifiedRecords == counters.records && counters.mismatches == 0L

  def json: String =
    val fields = Vector(
      "status" -> quote(if passed then "passed" else "failed"),
      "profile" -> quote(profile.cliName),
      "started_at" -> quote(startedAt.toString),
      "completed_at" -> quote(completedAt.toString),
      "configured_duration_seconds" -> configuredDurationSeconds.toString,
      "elapsed_seconds" -> String.format(Locale.ROOT, "%.3f", Double.box(elapsedSeconds)),
      "tenants" -> tenants.toString,
      "revision" -> quote(revision),
      "records" -> counters.records.toString,
      "verified_records" -> counters.verifiedRecords.toString,
      "recovered_records" -> counters.recoveredRecords.toString,
      "aborted_records" -> counters.abortedRecords.toString,
      "cycles" -> counters.cycles.toString,
      "mismatches" -> counters.mismatches.toString,
      "broker_restarts" -> counters.brokerRestarts.toString,
      "controller_failovers" -> counters.controllerFailovers.toString,
      "rebalances" -> counters.rebalances.toString,
      "tls_rotations" -> counters.tlsRotations.toString,
      "quota_reservations" -> counters.quotaReservations.toString,
      "quota_throttled" -> counters.quotaThrottled.toString,
      "quota_rejected" -> counters.quotaRejected.toString,
      "authentication_successes" -> counters.authenticationSuccesses.toString,
      "authentication_failures" -> counters.authenticationFailures.toString,
      "authenticated_tenants" -> counters.authenticatedTenants.toString,
      "peak_heap_bytes" -> counters.peakHeapBytes.toString,
      "restart_verified" -> counters.restartVerified.toString,
      "failure" -> failure.map(quote).getOrElse("null")
    )
    fields.map { case (name, value) => s"${quote(name)}:$value" }.mkString("{", ",", "}")

  private def quote(value: String): String =
    val escaped = StringBuilder(value.length + 16)
    value.foreach {
      case '"' => escaped.append("\\\"")
      case '\\' => escaped.append("\\\\")
      case '\b' => escaped.append("\\b")
      case '\f' => escaped.append("\\f")
      case '\n' => escaped.append("\\n")
      case '\r' => escaped.append("\\r")
      case '\t' => escaped.append("\\t")
      case character if character.isControl => escaped.append(f"\\u${character.toInt}%04x")
      case character => escaped.append(character)
    }
    s"\"$escaped\""

object SoakEvidence:
  def currentRevision(): String =
    def git(arguments: String*): String =
      val process = ProcessBuilder((Vector("git") ++ arguments).asJava).redirectErrorStream(true).start()
      if !process.waitFor(5L, TimeUnit.SECONDS) then
        process.destroyForcibly()
        throw IllegalStateException("git revision lookup timed out")
      val output = String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8).trim
      if process.exitValue() != 0 then throw IllegalStateException(s"git revision lookup failed: $output")
      output
    val revision = git("rev-parse", "HEAD")
    require(revision.matches("[0-9a-f]{40}"), "invalid Git revision")
    revision + (if git("status", "--porcelain").nonEmpty then "+working-tree" else "")

  def writeAtomically(path: Path, report: SoakReport): Unit =
    val absolute = path.toAbsolutePath.normalize()
    Option(absolute.getParent).foreach(Files.createDirectories(_))
    val parent = Option(absolute.getParent).getOrElse(Path.of(".").toAbsolutePath.normalize())
    val staged = Files.createTempFile(parent, s".${absolute.getFileName.toString}.", ".tmp")
    try
      Files.writeString(staged, report.json + System.lineSeparator(), StandardCharsets.UTF_8): Unit
      try Files.move(staged, absolute, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING): Unit
      catch
        case _: AtomicMoveNotSupportedException =>
          Files.move(staged, absolute, StandardCopyOption.REPLACE_EXISTING): Unit
    finally Files.deleteIfExists(staged): Unit

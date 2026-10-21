package cascade.qualification

import java.nio.charset.StandardCharsets
import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file.{AtomicMoveNotSupportedException, Files, Path}
import java.time.Instant

final case class AdjacentImageRollingReport(
    status: String,
    startedAt: Instant,
    elapsedSeconds: Double,
    baseline: ReleaseImage,
    candidate: ReleaseImage,
    records: Int,
    retries: Long,
    leaderTransitionNode: Int,
    rollbackBeforeUpgrade: Boolean,
    compatibleDowngradeAfterUpgrade: Boolean,
    exactRecovery: Boolean,
    phases: Vector[String],
    error: Option[String] = None
):
  require(Set("passed", "failed").contains(status), "rolling report status must be passed or failed")
  require(elapsedSeconds >= 0d, "rolling report elapsed time must be non-negative")
  require(records >= 0 && retries >= 0L, "rolling report counts must be non-negative")
  require(leaderTransitionNode >= 0, "rolling report leader node must be non-negative")

  def json: String =
    val phaseJson = phases.map(value => s"\"${escape(value)}\"").mkString("[", ",", "]")
    val errorJson = error.map(value => s"\"${escape(value)}\"").getOrElse("null")
    f"""{"status":"${escape(status)}","started_at":"$startedAt","elapsed_seconds":$elapsedSeconds%.3f,"baseline_image":"${escape(baseline.reference)}","baseline_image_id":"${baseline.imageId}","baseline_version":"${escape(baseline.version)}","baseline_revision":"${baseline.revision}","candidate_image":"${escape(candidate.reference)}","candidate_image_id":"${candidate.imageId}","candidate_version":"${escape(candidate.version)}","candidate_revision":"${candidate.revision}","records":$records,"idempotent_retries":$retries,"leader_transition_node":$leaderTransitionNode,"rollback_before_upgrade":$rollbackBeforeUpgrade,"compatible_downgrade_after_upgrade":$compatibleDowngradeAfterUpgrade,"exact_recovery":$exactRecovery,"phases":$phaseJson,"error":$errorJson}"""

  private def escape(value: String): String =
    value.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case character if character.isControl => f"\\u${character.toInt}%04x"
      case character => character.toString
    }

private[qualification] object AdjacentImageRollingReport:
  def writeAtomic(path: Path, report: AdjacentImageRollingReport): Unit =
    val target = path.toAbsolutePath.normalize()
    Option(target.getParent).foreach(Files.createDirectories(_))
    val temporary = target.resolveSibling(target.getFileName.toString + ".tmp")
    Files.writeString(temporary, report.json + System.lineSeparator(), StandardCharsets.UTF_8): Unit
    try Files.move(temporary, target, ATOMIC_MOVE, REPLACE_EXISTING): Unit
    catch case _: AtomicMoveNotSupportedException => Files.move(temporary, target, REPLACE_EXISTING): Unit

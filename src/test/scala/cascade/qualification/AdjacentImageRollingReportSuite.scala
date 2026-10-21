package cascade.qualification

import java.nio.file.Files
import java.time.Instant
import munit.FunSuite

final class AdjacentImageRollingReportSuite extends FunSuite:
  private val baseline = ReleaseImage("cascade:1.14.0", "sha256:" + "a" * 64, "1.14.0", "b" * 40, "time")
  private val candidate = ReleaseImage("cascade:1.15.0", "sha256:" + "c" * 64, "1.15.0", "d" * 40, "time")

  test("adjacent image report emits complete immutable evidence") {
    val report = AdjacentImageRollingReport(
      "passed", Instant.EPOCH, 1.25d, baseline, candidate, 1200, 3L, 2,
      rollbackBeforeUpgrade = true, compatibleDowngradeAfterUpgrade = true, exactRecovery = true,
      Vector("baseline", "quote-\"-and-newline-\n")
    )
    val json = report.json
    assert(json.contains("\"baseline_image_id\":\"sha256:" + "a" * 64 + "\""))
    assert(json.contains("\"idempotent_retries\":3"))
    assert(json.contains("quote-\\\"-and-newline-\\n"))
    assert(json.contains("\"exact_recovery\":true"))
  }

  test("adjacent image report is published atomically") {
    val directory = Files.createTempDirectory("cascade-adjacent-image-report")
    val target = directory.resolve("nested").resolve("report.json")
    val report = AdjacentImageRollingReport(
      "failed", Instant.EPOCH, 0d, baseline, candidate, 0, 0L, 0,
      rollbackBeforeUpgrade = false, compatibleDowngradeAfterUpgrade = false, exactRecovery = false,
      Vector.empty, Some("failure")
    )
    AdjacentImageRollingReport.writeAtomic(target, report)
    assertEquals(Files.readString(target).trim, report.json)
    assert(!Files.exists(target.resolveSibling("report.json.tmp")))
  }

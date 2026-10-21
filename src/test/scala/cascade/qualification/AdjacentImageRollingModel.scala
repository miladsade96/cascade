package cascade.qualification

import java.nio.file.{Path, Paths}
import java.time.Duration

private[qualification] final case class ReleaseImage(
    reference: String,
    imageId: String,
    version: String,
    revision: String,
    created: String
)

private[qualification] object ReleaseImage:
  private val InspectFormat =
    "{{.Id}}\t{{.Config.User}}\t{{index .Config.Labels \"org.opencontainers.image.version\"}}\t" +
      "{{index .Config.Labels \"org.opencontainers.image.revision\"}}\t" +
      "{{index .Config.Labels \"org.opencontainers.image.created\"}}\t" +
      "{{if .Config.Healthcheck}}health{{else}}missing{{end}}"

  def inspect(docker: DockerCli, reference: String, expectedVersion: String): ReleaseImage =
    require(reference.nonEmpty, "release image reference must not be empty")
    require(expectedVersion.matches("[0-9]+\\.[0-9]+\\.[0-9]+(?:-[0-9A-Za-z.-]+)?"), "invalid expected release version")
    val lines = docker.requireSuccess(
      Seq("image", "inspect", "--format", InspectFormat, reference),
      Duration.ofSeconds(30)
    )
    require(lines.size == 1, s"image inspection returned ${lines.size} lines")
    val fields = lines.head.split("\\t", -1).toVector
    require(fields.size == 6, "image inspection returned incomplete release metadata")
    val Vector(imageId, user, version, revision, created, health) = fields: @unchecked
    require(imageId.matches("sha256:[0-9a-f]{64}"), "release image ID is not an immutable SHA-256")
    require(user == "65532:65532", "release image must declare UID/GID 65532")
    require(version == expectedVersion, s"release image version $version does not match $expectedVersion")
    require(revision.matches("[0-9a-f]{40}"), "release image revision must be a full Git SHA")
    require(created.nonEmpty && created != "unknown", "release image creation time is missing")
    require(health == "health", "release image health check is missing")
    ReleaseImage(reference, imageId, version, revision, created)

final case class AdjacentImageRollingConfig(
    baselineImage: String,
    candidateImage: String,
    baselineVersion: String,
    candidateVersion: String,
    reportPath: Path,
    recordsPerPhase: Int = 25,
    transitionRecords: Int = 1000,
    dockerExecutable: String = "docker",
    keepResources: Boolean = false
):
  require(baselineImage.nonEmpty && candidateImage.nonEmpty, "rolling images must not be empty")
  require(baselineVersion.matches("[0-9]+\\.[0-9]+\\.[0-9]+"), "invalid baseline version")
  require(candidateVersion.matches("[0-9]+\\.[0-9]+\\.[0-9]+"), "invalid candidate version")
  require(baselineVersion != candidateVersion, "adjacent image versions must differ")
  require(recordsPerPhase > 0 && recordsPerPhase <= 10_000, "records per phase must be between 1 and 10000")
  require(transitionRecords >= recordsPerPhase && transitionRecords <= 100_000,
    "transition records must be between records per phase and 100000")
  require(dockerExecutable.nonEmpty, "Docker executable must not be empty")

object AdjacentImageRollingConfig:
  def parse(arguments: Array[String]): AdjacentImageRollingConfig =
    @annotation.tailrec
    def loop(values: List[String], options: Map[String, String], keep: Boolean): (Map[String, String], Boolean) =
      values match
        case Nil => options -> keep
        case "--keep-resources" :: tail => loop(tail, options, true)
        case option :: value :: tail if option.startsWith("--") =>
          require(!options.contains(option.drop(2)), s"duplicate rolling option: $option")
          loop(tail, options.updated(option.drop(2), value), keep)
        case option :: _ => throw IllegalArgumentException(s"unknown or incomplete adjacent-image option: $option")

    val (options, keep) = loop(arguments.toList, Map.empty, false)
    val allowed = Set(
      "baseline-image", "candidate-image", "baseline-version", "candidate-version", "report",
      "records-per-phase", "transition-records", "docker"
    )
    require(options.keySet.subsetOf(allowed), "unknown adjacent-image option")
    def required(name: String): String = options.getOrElse(name, throw IllegalArgumentException(s"missing --$name"))
    AdjacentImageRollingConfig(
      required("baseline-image"),
      required("candidate-image"),
      required("baseline-version"),
      required("candidate-version"),
      Paths.get(required("report")).toAbsolutePath.normalize(),
      options.get("records-per-phase").fold(25)(_.toInt),
      options.get("transition-records").fold(1000)(_.toInt),
      options.getOrElse("docker", "docker"),
      keep
    )

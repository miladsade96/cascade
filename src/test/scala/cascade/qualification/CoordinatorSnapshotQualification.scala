package cascade.qualification

import cascade.coordinator.*
import cascade.group.*
import cascade.delivery.*
import java.lang.management.ManagementFactory
import java.nio.file.{Files, Path}
import java.time.Instant
import scala.sys.process.*

/** In-process preparation benchmark, not a broker throughput or durability benchmark. */
object CoordinatorSnapshotQualification:
  final case class Sample(mode: String, trial: Int, millis: Double, cpuMillis: Double, allocatedBytes: Long, encoded: Long, reused: Long):
    def json: String = f"""{"mode":"$mode","trial":$trial,"milliseconds":$millis%.3f,"cpu_milliseconds":$cpuMillis%.3f,"allocated_bytes":$allocatedBytes,"encoded_shards":$encoded,"reused_shards":$reused}"""

  def main(arguments: Array[String]): Unit =
    require(arguments.length % 2 == 0, "options require values")
    val pairs = arguments.grouped(2).map(a => a(0) -> a(1)).toVector
    require(pairs.map(_._1).distinct.size == pairs.size, "duplicate option")
    require(pairs.forall(p => Set("--groups", "--iterations", "--report")(p._1)), "unknown option")
    val options = pairs.toMap
    val groups = options.getOrElse("--groups", "1000").toInt
    val iterations = options.getOrElse("--iterations", "500").toInt
    val samples = run(groups, iterations)
    val revision = Seq("git", "rev-parse", "HEAD").!!.trim +
      (if Seq("git", "status", "--porcelain").!!.trim.nonEmpty then "+working-tree" else "")
    val json = s"""{"status":"passed","scope":"candidate preparation only; excludes live group capture, quorum, disk and installation","started_at":"${Instant.now()}","revision":"$revision","groups":$groups,"producers":$groups,"iterations":$iterations,"trials":4,"warmup_per_trial":${math.min(100, iterations)},"byte_exact_verified":$iterations,"java_version":"${System.getProperty("java.version")}","samples":${samples.map(_.json).mkString("[", ",", "]")}}"""
    val report = Path.of(options.getOrElse("--report", "artifacts/coordinator-snapshot.json")).toAbsolutePath
    Files.createDirectories(report.getParent)
    Files.writeString(report, json + "\n")
    println(s"COORDINATOR_SNAPSHOT_RESULT $json")

  def run(groups: Int, iterations: Int): Vector[Sample] =
    require(groups > 0 && groups <= 10000, "group count must be 1..10000")
    require(iterations > 0 && iterations <= 10000, "iterations must be 1..10000")
    val offsets = Vector.tabulate(groups)(i => OffsetCommitValue(GroupOffsetKey(s"group-$i", "events", 0), CommittedOffset(0L, -1, None, 1L)))
    val producers = Vector.tabulate(groups)(i => ProducerRegistration(i.toLong + 1L, 0, Some(s"txn-$i"), 10000))
    val initial = GroupImage(0L, Vector.empty, offsets) -> DeliveryImage(0L, groups.toLong + 1L, producers, Vector.empty, Vector.empty)
    val inputs = (1 to iterations).scanLeft(initial) { case ((group, delivery), i) =>
      val index = i % groups
      val nextGroups = group.copy(version = i.toLong, offsets = group.offsets.updated(index,
        group.offsets(index).copy(value = CommittedOffset(i.toLong, -1, None, i.toLong))))
      val nextDelivery = if i % 4 != 0 then delivery else delivery.copy(version = i.toLong,
        producers = delivery.producers.updated(index, delivery.producers(index).copy(producerEpoch = (i % 30000).toShort)))
      nextGroups -> nextDelivery
    }.tail.toVector
    def legacy(input: (GroupImage, DeliveryImage)): Vector[Vector[Byte]] =
      CoordinatorShardState.payloads(GroupCodec.encode(input._1).toVector, DeliveryCodec.encode(input._2).toVector)
    val verified = CoordinatorSnapshotCache()
    inputs.foreach { case input @ (group, delivery) =>
      require(verified.capture(group, delivery).payloads == legacy(input), "cached and full serialization disagree")
    }
    val bean = ManagementFactory.getThreadMXBean
    val allocation = bean match
      case value: com.sun.management.ThreadMXBean if value.isThreadAllocatedMemorySupported && value.isThreadAllocatedMemoryEnabled => Some(value)
      case _ => None
    def allocated: Long = allocation.map(_.getThreadAllocatedBytes(Thread.currentThread().threadId())).getOrElse(-1L)
    def cpu: Long = if bean.isCurrentThreadCpuTimeSupported && bean.isThreadCpuTimeEnabled then bean.getCurrentThreadCpuTime else -1L
    (0 until 4).toVector.flatMap { trial =>
      val modes = if trial % 2 == 0 then Vector("full", "cached") else Vector("cached", "full")
      modes.map { mode =>
        val cache = CoordinatorSnapshotCache()
        var encoded = 0L
        var reused = 0L
        var bytes = 0L
        def capture(input: (GroupImage, DeliveryImage)): Unit =
          if mode == "full" then
            bytes += legacy(input).iterator.map(_.size.toLong).sum
            encoded += CoordinatorShard.Count
          else
            val result = cache.capture(input._1, input._2)
            bytes += result.payloads.iterator.map(_.size.toLong).sum
            encoded += result.encoded
            reused += result.reused
        inputs.take(math.min(100, iterations)).foreach(capture)
        encoded = 0L
        reused = 0L
        val beforeBytes = allocated
        val beforeCpu = cpu
        val before = System.nanoTime()
        inputs.foreach(capture)
        val nanos = System.nanoTime() - before
        val cpuNanos = cpu - beforeCpu
        val allocatedBytes = if beforeBytes < 0L then -1L else allocated - beforeBytes
        require(bytes > 0L, "benchmark must consume its output")
        Sample(mode, trial, nanos / 1000000d, if beforeCpu < 0L then -1d else cpuNanos / 1000000d, allocatedBytes, encoded, reused)
      }
    }

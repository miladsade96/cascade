package cascade.storage

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption.{TRUNCATE_EXISTING, WRITE}
import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.jdk.CollectionConverters.*

final case class TopicPartition(topic: String, partition: Int)

enum CreateTopicResult:
  case Created, AlreadyExists, InvalidName, InvalidPartitions

final class TopicRegistry(
    dataDirectory: Path,
    maxSegmentBytes: Long,
    flushPolicy: FlushPolicy = FlushPolicy.Periodic,
    flushIntervalMillis: Long = 1000L,
    flushBytes: Long = 64L * 1024 * 1024,
    lifecycleConfig: StorageLifecycleConfig = StorageLifecycleConfig(),
    backgroundError: (String, Throwable) => Unit = (event, error) =>
      System.err.println(s"Cascade $event: ${error.getMessage}")
) extends AutoCloseable:
  require(flushIntervalMillis > 0, "flush interval must be positive")
  require(flushBytes > 0, "flush bytes must be positive")

  private val topics = ConcurrentHashMap[String, Vector[PartitionLog]]()
  private val topicPolicies = ConcurrentHashMap[String, TopicLifecyclePolicy]()
  private val closed = AtomicBoolean(false)
  private val flushQueued = AtomicBoolean(false)
  private val backgroundFailure = AtomicReference[Throwable]()
  private val snapshotBarrier = ReentrantReadWriteLock(true)
  private val PartitionDirectory = "partition-([0-9]+)".r
  private val policyPath = dataDirectory.resolve(".cascade").resolve("topic-lifecycle.conf")
  private val flusher: Option[ScheduledExecutorService] = flushPolicy match
    case FlushPolicy.Periodic =>
      Some(Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-log-flusher").factory()))
    case FlushPolicy.Sync => None
  private val lifecycleExecutor: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-storage-lifecycle").factory())

  Files.createDirectories(dataDirectory)
  loadTopicPolicies()
  discoverTopics()
  startPeriodicFlusher()
  startLifecycleScheduler()

  def topicNames: Vector[String] =
    ensureHealthy()
    topics.keySet().asScala.toVector.sorted

  def partitions(topic: String): Option[Vector[PartitionLog]] =
    ensureHealthy()
    Option(topics.get(topic))

  def partition(topic: String, index: Int): Option[PartitionLog] =
    partitions(topic).flatMap(_.lift(index))

  def effectiveLifecyclePolicy(topic: String): Option[TopicLifecyclePolicy] =
    Option.when(topics.containsKey(topic)) {
      Option(topicPolicies.get(topic)).getOrElse(TopicLifecyclePolicy.from(lifecycleConfig))
    }

  def configuredLifecyclePolicy(topic: String): Option[TopicLifecyclePolicy] = Option(topicPolicies.get(topic))

  def configureLifecycle(topic: String, policy: TopicLifecyclePolicy): Either[String, Unit] = synchronized {
    ensureHealthy()
    Option(topics.get(topic)) match
      case None => Left("topic does not exist")
      case Some(logs) if Option(topicPolicies.get(topic)).contains(policy) =>
        val effective = policy.applyTo(lifecycleConfig)
        logs.foreach(_.updateLifecycleConfig(effective))
        Right(())
      case Some(logs) =>
        try
          val next = topicPolicies.asScala.toMap.updated(topic, policy)
          persistTopicPolicies(next)
          topicPolicies.put(topic, policy)
          val effective = policy.applyTo(lifecycleConfig)
          logs.foreach(_.updateLifecycleConfig(effective))
          Right(())
        catch case error: Throwable => Left(Option(error.getMessage).getOrElse(error.getClass.getSimpleName))
  }

  def validateTopicName(name: String): Boolean = validTopicName(name)

  def createTopic(name: String, partitionCount: Int): CreateTopicResult = synchronized {
    ensureHealthy()
    if !validTopicName(name) then CreateTopicResult.InvalidName
    else if partitionCount <= 0 then CreateTopicResult.InvalidPartitions
    else if topics.containsKey(name) then CreateTopicResult.AlreadyExists
    else
      val logs = Vector.tabulate(partitionCount)(index => openPartition(name, index))
      topics.put(name, logs)
      CreateTopicResult.Created
  }

  def getOrCreate(name: String, partitionCount: Int = 1): Option[Vector[PartitionLog]] = synchronized {
    ensureHealthy()
    Option(topics.get(name)).orElse {
      createTopic(name, partitionCount) match
        case CreateTopicResult.Created | CreateTopicResult.AlreadyExists => Option(topics.get(name))
        case _                                                          => None
    }
  }

  override def close(): Unit =
    if closed.compareAndSet(false, true) then
      lifecycleExecutor.shutdown()
      if !lifecycleExecutor.awaitTermination(30, TimeUnit.SECONDS) then
        lifecycleExecutor.shutdownNow(): Unit
        lifecycleExecutor.awaitTermination(30, TimeUnit.SECONDS): Unit
      flusher.foreach { executor =>
        executor.shutdown()
        if !executor.awaitTermination(30, TimeUnit.SECONDS) then
          executor.shutdownNow(): Unit
          executor.awaitTermination(30, TimeUnit.SECONDS): Unit
      }
      topics.values().asScala.foreach(_.foreach(_.close()))

  def flushStatistics: FlushStatistics =
    topics.values().asScala
      .flatMap(_.iterator)
      .map(_.flushStatistics)
      .foldLeft(FlushStatistics.Empty)(_ + _)

  def lifecycleStatistics: LifecycleStatistics =
    topics.values().asScala
      .flatMap(_.iterator)
      .map(_.lifecycleStatistics)
      .foldLeft(LifecycleStatistics.Empty)(_ + _)

  /** Stops lifecycle/flush workers, forces all logs, and keeps their files immutable for the callback. */
  def withSnapshotBarrier[A](callback: Map[TopicPartition, Long] => A): A =
    val lock = snapshotBarrier.writeLock()
    lock.lock()
    try
      ensureHealthy()
      val watermarks = topics.asScala.toVector.sortBy(_._1).flatMap { case (topic, logs) =>
        logs.zipWithIndex.map { case (log, partition) => TopicPartition(topic, partition) -> log.flushForSnapshot() }
      }.toMap
      callback(watermarks)
    finally lock.unlock()

  private def discoverTopics(): Unit =
    val directories = Files.list(dataDirectory)
    try
      directories.iterator().asScala.filter(path => Files.isDirectory(path)).foreach { topicDirectory =>
        val name = topicDirectory.getFileName.toString
        if validTopicName(name) then
          val partitions = Files.list(topicDirectory)
          try
            val indices = partitions.iterator().asScala
              .filter(path => Files.isDirectory(path))
              .flatMap { path =>
                path.getFileName.toString match
                  case PartitionDirectory(index) => Some(index.toInt)
                  case _                         => None
              }
              .toVector
              .sorted
            if indices.nonEmpty && indices == indices.indices.toVector then
              topics.put(name, indices.map(index => openPartition(name, index))): Unit
          finally partitions.close()
      }
    finally directories.close()

  private def openPartition(topic: String, partition: Int): PartitionLog =
    PartitionLog(
      dataDirectory.resolve(topic).resolve(s"partition-$partition"),
      maxSegmentBytes,
      flushPolicy,
      flushIntervalMillis,
      flushBytes,
      requestFlush,
      Option(topicPolicies.get(topic)).map(_.applyTo(lifecycleConfig)).getOrElse(lifecycleConfig)
    )

  private def loadTopicPolicies(): Unit =
    if Files.exists(policyPath) then
      val parsed = Files.readAllLines(policyPath, StandardCharsets.UTF_8).asScala.iterator.zipWithIndex.flatMap { case (raw, index) =>
        val line = raw.trim
        if line.isEmpty || line.startsWith("#") then None
        else
          val fields = line.split("\\s+", -1)
          if fields.length != 4 || !validTopicName(fields(0)) then
            throw IllegalArgumentException(s"invalid topic lifecycle policy at ${policyPath.getFileName}:${index + 1}")
          Some(fields(0) -> TopicLifecyclePolicy(CleanupPolicy.parse(fields(1)), fields(2).toLong, fields(3).toLong))
      }.toVector
      if parsed.map(_._1).distinct.size != parsed.size then throw IllegalArgumentException("duplicate topic lifecycle policy")
      parsed.foreach { case (topic, policy) => topicPolicies.put(topic, policy): Unit }

  private def persistTopicPolicies(policies: Map[String, TopicLifecyclePolicy]): Unit =
    val parent = policyPath.getParent
    Files.createDirectories(parent)
    val temporary = Files.createTempFile(parent, policyPath.getFileName.toString + ".", ".tmp")
    try
      val content = policies.toVector.sortBy(_._1).map { case (topic, policy) =>
        s"$topic ${cleanupPolicyName(policy.cleanupPolicy)} ${policy.retentionMillis} ${policy.retentionBytes}"
      }.mkString("", System.lineSeparator(), System.lineSeparator())
      val channel = FileChannel.open(temporary, WRITE, TRUNCATE_EXISTING)
      try
        val bytes = ByteBuffer.wrap(content.getBytes(StandardCharsets.UTF_8))
        while bytes.hasRemaining do channel.write(bytes): Unit
        channel.force(true)
      finally channel.close()
      AtomicFileLifecycle.replace(temporary, policyPath)
    finally Files.deleteIfExists(temporary): Unit

  private def cleanupPolicyName(policy: CleanupPolicy): String = policy match
    case CleanupPolicy.Delete        => "delete"
    case CleanupPolicy.Compact       => "compact"
    case CleanupPolicy.CompactDelete => "compact,delete"

  private def startPeriodicFlusher(): Unit =
    flusher.foreach { executor =>
      val checkIntervalMillis = math.min(100L, math.max(10L, flushIntervalMillis / 4L))
      executor.scheduleWithFixedDelay(
        () => flushDueLogs(),
        checkIntervalMillis,
        checkIntervalMillis,
        TimeUnit.MILLISECONDS
      ): Unit
    }

  private def startLifecycleScheduler(): Unit =
    lifecycleExecutor.scheduleWithFixedDelay(
      () => runLifecycle(),
      lifecycleConfig.lifecycleIntervalMillis,
      lifecycleConfig.lifecycleIntervalMillis,
      TimeUnit.MILLISECONDS
    ): Unit

  private def runLifecycle(): Unit =
    val lock = snapshotBarrier.readLock()
    lock.lock()
    try
      topics.values().asScala.foreach { logs =>
        logs.foreach { log =>
          try log.runLifecycle()
          catch
            case error: Throwable =>
              backgroundFailure.compareAndSet(null, error): Unit
              backgroundError("storage_lifecycle_error", error)
        }
      }
    finally lock.unlock()

  private def requestFlush(): Unit =
    flusher.foreach { executor =>
      if !closed.get() && flushQueued.compareAndSet(false, true) then
        try executor.execute(() => flushDueLogs()): Unit
        catch
          case _: java.util.concurrent.RejectedExecutionException => flushQueued.set(false)
    }

  private def flushDueLogs(): Unit =
    flushQueued.set(false)
    val lock = snapshotBarrier.readLock()
    lock.lock()
    try
      val now = System.nanoTime()
      topics.values().asScala.foreach { logs =>
        logs.foreach { log =>
          try log.flushIfNeeded(now)
          catch
            case error: Throwable =>
              backgroundFailure.compareAndSet(null, error): Unit
              backgroundError("log_flush_error", error)
        }
      }
    finally lock.unlock()

  private def ensureHealthy(): Unit =
    val failure = backgroundFailure.get()
    if failure != null then throw IllegalStateException("background log flushing failed", failure)

  private def validTopicName(name: String): Boolean =
    name.nonEmpty && name.length <= 249 && name != "." && name != ".." &&
      name.forall(character => character.isLetterOrDigit || character == '.' || character == '_' || character == '-')

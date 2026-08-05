package cascade.storage

import java.nio.file.{Files, Path}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import java.util.concurrent.{ConcurrentHashMap, Executors, ScheduledExecutorService, TimeUnit}
import scala.jdk.CollectionConverters.*

final case class TopicPartition(topic: String, partition: Int)

enum CreateTopicResult:
  case Created, AlreadyExists, InvalidName, InvalidPartitions

final class TopicRegistry(
    dataDirectory: Path,
    maxSegmentBytes: Long,
    flushPolicy: FlushPolicy = FlushPolicy.Periodic,
    flushIntervalMillis: Long = 1000L,
    flushBytes: Long = 64L * 1024 * 1024
) extends AutoCloseable:
  require(flushIntervalMillis > 0, "flush interval must be positive")
  require(flushBytes > 0, "flush bytes must be positive")

  private val topics = ConcurrentHashMap[String, Vector[PartitionLog]]()
  private val closed = AtomicBoolean(false)
  private val flushQueued = AtomicBoolean(false)
  private val backgroundFailure = AtomicReference[Throwable]()
  private val PartitionDirectory = "partition-([0-9]+)".r
  private val flusher: Option[ScheduledExecutorService] = flushPolicy match
    case FlushPolicy.Periodic =>
      Some(Executors.newSingleThreadScheduledExecutor(Thread.ofPlatform().daemon().name("cascade-log-flusher").factory()))
    case FlushPolicy.Sync => None

  Files.createDirectories(dataDirectory)
  discoverTopics()
  startPeriodicFlusher()

  def topicNames: Vector[String] =
    ensureHealthy()
    topics.keySet().asScala.toVector.sorted

  def partitions(topic: String): Option[Vector[PartitionLog]] =
    ensureHealthy()
    Option(topics.get(topic))

  def partition(topic: String, index: Int): Option[PartitionLog] =
    partitions(topic).flatMap(_.lift(index))

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
      requestFlush
    )

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

  private def requestFlush(): Unit =
    flusher.foreach { executor =>
      if !closed.get() && flushQueued.compareAndSet(false, true) then
        try executor.execute(() => flushDueLogs()): Unit
        catch
          case _: java.util.concurrent.RejectedExecutionException => flushQueued.set(false)
    }

  private def flushDueLogs(): Unit =
    flushQueued.set(false)
    val now = System.nanoTime()
    topics.values().asScala.foreach { logs =>
      logs.foreach { log =>
        try log.flushIfNeeded(now)
        catch
          case error: Throwable =>
            backgroundFailure.compareAndSet(null, error): Unit
            System.err.println(s"Cascade log flush error: ${error.getMessage}")
      }
    }

  private def ensureHealthy(): Unit =
    val failure = backgroundFailure.get()
    if failure != null then throw IllegalStateException("background log flushing failed", failure)

  private def validTopicName(name: String): Boolean =
    name.nonEmpty && name.length <= 249 && name != "." && name != ".." &&
      name.forall(character => character.isLetterOrDigit || character == '.' || character == '_' || character == '-')

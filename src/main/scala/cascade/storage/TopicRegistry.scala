package cascade.storage

import java.nio.file.{Files, Path}
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters.*

final case class TopicPartition(topic: String, partition: Int)

enum CreateTopicResult:
  case Created, AlreadyExists, InvalidName, InvalidPartitions

final class TopicRegistry(dataDirectory: Path, maxSegmentBytes: Long) extends AutoCloseable:
  private val topics = ConcurrentHashMap[String, Vector[PartitionLog]]()
  Files.createDirectories(dataDirectory)
  discoverTopics()

  def topicNames: Vector[String] = topics.keySet().asScala.toVector.sorted

  def partitions(topic: String): Option[Vector[PartitionLog]] = Option(topics.get(topic))

  def partition(topic: String, index: Int): Option[PartitionLog] =
    partitions(topic).flatMap(_.lift(index))

  def createTopic(name: String, partitionCount: Int): CreateTopicResult = synchronized {
    if !validTopicName(name) then CreateTopicResult.InvalidName
    else if partitionCount <= 0 then CreateTopicResult.InvalidPartitions
    else if topics.containsKey(name) then CreateTopicResult.AlreadyExists
    else
      val logs = Vector.tabulate(partitionCount)(index => openPartition(name, index))
      topics.put(name, logs)
      CreateTopicResult.Created
  }

  def getOrCreate(name: String, partitionCount: Int = 1): Option[Vector[PartitionLog]] = synchronized {
    Option(topics.get(name)).orElse {
      createTopic(name, partitionCount) match
        case CreateTopicResult.Created | CreateTopicResult.AlreadyExists => Option(topics.get(name))
        case _                                                          => None
    }
  }

  override def close(): Unit = topics.values().asScala.foreach(_.foreach(_.close()))

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
    PartitionLog(dataDirectory.resolve(topic).resolve(s"partition-$partition"), maxSegmentBytes)

  private def validTopicName(name: String): Boolean =
    name.nonEmpty && name.length <= 249 && name != "." && name != ".." &&
      name.forall(character => character.isLetterOrDigit || character == '.' || character == '_' || character == '-')

  private val PartitionDirectory = "partition-([0-9]+)".r

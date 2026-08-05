package cascade.broker

import cascade.storage.FlushPolicy
import java.nio.file.{Path, Paths}

final case class BrokerConfig(
    bindHost: String = "0.0.0.0",
    port: Int = 9092,
    advertisedHost: String = "localhost",
    advertisedPort: Option[Int] = None,
    dataDirectory: Path = Paths.get("data"),
    maxRequestBytes: Int = 100 * 1024 * 1024,
    segmentBytes: Long = 128L * 1024 * 1024,
    flushPolicy: FlushPolicy = FlushPolicy.Periodic,
    flushIntervalMillis: Long = 1000L,
    flushBytes: Long = 64L * 1024 * 1024,
    nodeId: Int = 1,
    autoCreateTopics: Boolean = true
):
  require(port >= 0 && port <= 65535, "port must be between 0 and 65535")
  require(advertisedPort.forall(value => value > 0 && value <= 65535), "advertised port must be valid")
  require(maxRequestBytes >= 1024, "max request size must be at least 1 KiB")
  require(flushIntervalMillis > 0, "flush interval must be positive")
  require(flushBytes > 0, "flush bytes must be positive")

object BrokerConfig:
  def parse(arguments: Array[String]): BrokerConfig =
    @annotation.tailrec
    def loop(remaining: List[String], config: BrokerConfig): BrokerConfig = remaining match
      case Nil => config
      case "--host" :: value :: tail => loop(tail, config.copy(bindHost = value))
      case "--port" :: value :: tail => loop(tail, config.copy(port = value.toInt))
      case "--advertised-host" :: value :: tail => loop(tail, config.copy(advertisedHost = value))
      case "--advertised-port" :: value :: tail => loop(tail, config.copy(advertisedPort = Some(value.toInt)))
      case "--data-dir" :: value :: tail => loop(tail, config.copy(dataDirectory = Paths.get(value)))
      case "--max-request-bytes" :: value :: tail => loop(tail, config.copy(maxRequestBytes = value.toInt))
      case "--segment-bytes" :: value :: tail => loop(tail, config.copy(segmentBytes = value.toLong))
      case "--flush-policy" :: value :: tail => loop(tail, config.copy(flushPolicy = FlushPolicy.parse(value)))
      case "--flush-interval-ms" :: value :: tail => loop(tail, config.copy(flushIntervalMillis = value.toLong))
      case "--flush-bytes" :: value :: tail => loop(tail, config.copy(flushBytes = value.toLong))
      case "--node-id" :: value :: tail => loop(tail, config.copy(nodeId = value.toInt))
      case "--no-auto-create" :: tail => loop(tail, config.copy(autoCreateTopics = false))
      case option :: _ => throw IllegalArgumentException(s"unknown or incomplete option: $option")
    loop(arguments.toList, BrokerConfig())

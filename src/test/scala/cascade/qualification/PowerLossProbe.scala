package cascade.qualification

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption.{APPEND, CREATE, WRITE}
import java.nio.file.{Files, Path, Paths}
import java.security.MessageDigest
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TopicExistsException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import scala.jdk.CollectionConverters.*

/**
 * Two-phase physical qualification probe. Put the evidence file on an independent control device,
 * run `write`, cut host/device power externally, restart the cluster, then run `verify`.
 */
object PowerLossProbe:
  private final case class Config(
      mode: String = "",
      bootstrap: String = "",
      topic: String = "cascade-power-loss",
      evidence: Path = Paths.get("power-loss.evidence"),
      durationSeconds: Long = Long.MaxValue,
      payloadBytes: Int = 1024
  )

  def main(arguments: Array[String]): Unit =
    val config = parse(arguments.toList, Config())
    require(config.bootstrap.nonEmpty, "--bootstrap is required")
    config.mode match
      case "write"  => writeUntilInterrupted(config)
      case "verify" => verify(config)
      case _ => throw IllegalArgumentException("--mode must be write or verify")

  private def writeUntilInterrupted(config: Config): Unit =
    createTopic(config)
    Option(config.evidence.toAbsolutePath.getParent).foreach(Files.createDirectories(_))
    val evidence = FileChannel.open(config.evidence, CREATE, WRITE, APPEND)
    val producer = KafkaProducer[Array[Byte], Array[Byte]](producerProperties(config.bootstrap))
    try
      var sequence = existingEvidence(config.evidence).size.toLong
      val deadline =
        if config.durationSeconds == Long.MaxValue then Long.MaxValue
        else System.nanoTime() + Duration.ofSeconds(config.durationSeconds).toNanos
      while System.nanoTime() < deadline do
        val value = payload(sequence, config.payloadBytes)
        val metadata = producer.send(ProducerRecord(config.topic, 0, longBytes(sequence), value)).get(30, TimeUnit.SECONDS)
        if metadata.offset() != sequence then throw IllegalStateException(s"expected offset $sequence, got ${metadata.offset()}")
        val line = s"$sequence\t${sha256(value)}${System.lineSeparator()}".getBytes(StandardCharsets.UTF_8)
        val buffer = ByteBuffer.wrap(line)
        while buffer.hasRemaining do evidence.write(buffer): Unit
        evidence.force(true)
        sequence += 1L
        if sequence % 1000L == 0L then println(s"POWER_LOSS_ARMED acknowledged_and_witnessed=$sequence")
    finally
      producer.close(Duration.ofSeconds(5))
      evidence.close()

  private def verify(config: Config): Unit =
    val expected = existingEvidence(config.evidence)
    require(expected.nonEmpty, "evidence file contains no acknowledged records")
    val consumer = KafkaConsumer[Array[Byte], Array[Byte]](consumerProperties(config.bootstrap))
    try
      val partition = TopicPartition(config.topic, 0)
      consumer.assign(java.util.List.of(partition))
      consumer.seekToBeginning(java.util.List.of(partition))
      var verified = 0
      val deadline = System.nanoTime() + Duration.ofMinutes(5).toNanos
      while verified < expected.size && System.nanoTime() < deadline do
        consumer.poll(Duration.ofMillis(250)).iterator().asScala.foreach { record =>
          if verified < expected.size then
            val (sequence, checksum) = expected(verified)
            if record.offset() != sequence || sha256(record.value()) != checksum then
              throw IllegalStateException(s"durability mismatch at witnessed record $sequence")
            verified += 1
        }
      if verified != expected.size then throw IllegalStateException(s"only recovered $verified/${expected.size} witnessed records")
      println(s"POWER_LOSS_QUALIFIED witnessed_records=$verified")
    finally consumer.close()

  private def createTopic(config: Config): Unit =
    val admin = Admin.create(properties(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> config.bootstrap))
    try
      try admin.createTopics(java.util.List.of(NewTopic(config.topic, 1, 3.toShort))).all().get(30, TimeUnit.SECONDS): Unit
      catch case error: java.util.concurrent.ExecutionException if error.getCause.isInstanceOf[TopicExistsException] => ()
    finally admin.close(Duration.ofSeconds(5))

  @annotation.tailrec
  private def parse(arguments: List[String], config: Config): Config = arguments match
    case Nil => config
    case "write" :: tail if config.mode.isEmpty => parse(tail, config.copy(mode = "write"))
    case "verify" :: tail if config.mode.isEmpty => parse(tail, config.copy(mode = "verify"))
    case "--mode" :: value :: tail => parse(tail, config.copy(mode = value))
    case "--bootstrap" :: value :: tail => parse(tail, config.copy(bootstrap = value))
    case "--topic" :: value :: tail => parse(tail, config.copy(topic = value))
    case "--evidence" :: value :: tail => parse(tail, config.copy(evidence = Paths.get(value)))
    case "--duration-seconds" :: value :: tail => parse(tail, config.copy(durationSeconds = value.toLong))
    case "--payload-bytes" :: value :: tail => parse(tail, config.copy(payloadBytes = value.toInt))
    case option :: _ => throw IllegalArgumentException(s"unknown or incomplete power-loss option: $option")

  private def existingEvidence(path: Path): Vector[(Long, String)] =
    if !Files.exists(path) then Vector.empty
    else Files.readAllLines(path, StandardCharsets.UTF_8).asScala.toVector.zipWithIndex.map { case (line, index) =>
      line.split("\\t", -1).toVector match
        case Vector(sequence, checksum) if checksum.matches("[0-9a-f]{64}") && sequence.toLong == index.toLong =>
          sequence.toLong -> checksum
        case _ => throw IllegalArgumentException(s"invalid evidence line ${index + 1}")
    }

  private def producerProperties(bootstrap: String): Properties = properties(
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[ByteArraySerializer].getName,
    ProducerConfig.ACKS_CONFIG -> "all",
    ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> "true",
    "enable.metrics.push" -> "false"
  )

  private def consumerProperties(bootstrap: String): Properties = properties(
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap,
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[ByteArrayDeserializer].getName,
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "false",
    "enable.metrics.push" -> "false"
  )

  private def properties(values: (String, String)*): Properties =
    val result = Properties()
    values.foreach { case (name, value) => result.put(name, value): Unit }
    result

  private def payload(sequence: Long, bytes: Int): Array[Byte] =
    require(bytes >= 16, "payload must be at least 16 bytes")
    val value = new Array[Byte](bytes)
    ByteBuffer.wrap(value).putLong(sequence)
    var index = 8
    while index < value.length do
      value(index) = ((sequence * 31L + index) & 0xff).toByte
      index += 1
    value

  private def longBytes(value: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(value).array()

  private def sha256(value: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(value).map(byte => f"${byte & 0xff}%02x").mkString

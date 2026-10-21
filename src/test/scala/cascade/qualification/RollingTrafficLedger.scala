package cascade.qualification

import scala.collection.mutable.ArrayBuffer

private[qualification] final class RollingTrafficLedger:
  private val values = ArrayBuffer.empty[String]

  def reserve(label: String, count: Int): Vector[(Long, String)] = synchronized {
    require(label.nonEmpty, "rolling traffic label must not be empty")
    require(count > 0, "rolling traffic count must be positive")
    val start = values.size
    val planned = Vector.tabulate(count) { index =>
      val offset = start.toLong + index
      offset -> s"$offset:$label:$index"
    }
    values ++= planned.map(_._2)
    planned
  }

  def assertAcknowledged(planned: Vector[(Long, String)], offsets: Vector[Long]): Unit =
    require(planned.size == offsets.size, "rolling acknowledgement count changed")
    planned.map(_._1).zip(offsets).foreach { case (expected, actual) =>
      require(actual == expected, s"rolling acknowledgement offset mismatch: expected $expected, got $actual")
    }

  def assertObserved(observed: Vector[(Long, String)]): Unit = synchronized {
    require(observed.size == values.size, s"rolling record count mismatch: expected ${values.size}, got ${observed.size}")
    observed.zipWithIndex.foreach { case ((offset, value), index) =>
      require(offset == index.toLong, s"rolling log offset is not contiguous: expected $index, got $offset")
      require(value == values(index), s"rolling value mismatch at offset $offset")
    }
  }

  def expectedValues: Vector[String] = synchronized(values.toVector)
  def size: Int = synchronized(values.size)

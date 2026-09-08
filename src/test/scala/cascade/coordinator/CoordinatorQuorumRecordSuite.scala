package cascade.coordinator

import munit.FunSuite

final class CoordinatorQuorumRecordSuite extends FunSuite:
  private val transaction = CoordinatorTransactionId(1L, 2L)
  private val delta = CoordinatorDelta(3L, Vector(
    CoordinatorShardUpdate(7, 0L, Vector(1.toByte)),
    CoordinatorShardUpdate(2, 4L, Vector(2.toByte))
  ))

  test("prepare records expose a sorted distinct shard set") {
    val record = CoordinatorQuorumRecord.prepare(transaction, delta)
    assertEquals(record.phase, CoordinatorQuorumPhase.Prepare)
    assertEquals(record.shards, Vector(2, 7))
    assertEquals(record.delta, Some(delta))
  }

  test("decision markers never carry mutable coordinator state") {
    CoordinatorQuorumPhase.values.filterNot(_ == CoordinatorQuorumPhase.Prepare).foreach { phase =>
      val record = CoordinatorQuorumRecord.marker(transaction, phase)
      assertEquals(record.delta, None)
      assertEquals(record.shards, Vector.empty)
    }
    intercept[IllegalArgumentException](CoordinatorQuorumRecord.marker(transaction, CoordinatorQuorumPhase.Prepare))
    intercept[IllegalArgumentException](CoordinatorQuorumRecord(transaction, CoordinatorQuorumPhase.Decide, Some(delta)))
  }

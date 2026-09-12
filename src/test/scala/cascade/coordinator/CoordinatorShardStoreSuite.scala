package cascade.coordinator

import cascade.cluster.{ClusterNode, CoordinatorMetadata, QuorumMembership}
import cascade.group.{CommittedOffset, GroupCodec, GroupImage, GroupOffsetKey, GroupShardCodec, OffsetCommitValue}
import cascade.protocol.{ByteWriter, Errors}
import java.nio.file.Files
import munit.FunSuite

final class CoordinatorShardStoreSuite extends FunSuite:
  private val baseline = CoordinatorMetadata.Empty
  private val certificate = CoordinatorDecisionCertificate.from(
    QuorumMembership.bootstrap(Vector(ClusterNode(1, "one", 1), ClusterNode(2, "two", 2), ClusterNode(3, "three", 3))),
    Set(1, 2)
  )

  private def certify(store: CoordinatorShardStore, transaction: CoordinatorTransactionId): Unit =
    assertEquals(store.decide(transaction), Errors.None)
    assertEquals(store.commitDecision(transaction, certificate), Errors.None)

  test("finalizes one shard without rewriting unrelated shard journals") {
    val directory = Files.createTempDirectory("cascade-coordinator-store")
    val (group, delta) = groupDelta("orders", 41L, 7L)
    val transaction = CoordinatorTransactionId(1L, 1L)
    val store = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(store.prepare(transaction, delta, 7L), Errors.None)
      certify(store, transaction)
      val committed = store.finalizeTransaction(transaction).toOption.get
      assertEquals(committed.shardVersion(CoordinatorShard.group(group)), 1L)
      assertEquals(committed.groupImage.offsets.map(_.value.offset), Vector(41L))
      assertEquals(Files.list(directory).count(), 1L)
      assertEquals(store.snapshot.finalized, 1L)
      assertEquals(store.snapshot.pending, 0)
    finally store.close()
  }

  test("recovers a decided transaction and finalizes it after restart") {
    val directory = Files.createTempDirectory("cascade-coordinator-decided")
    val (_, delta) = groupDelta("payments", 82L, 9L)
    val transaction = CoordinatorTransactionId(2L, 2L)
    val first = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(first.prepare(transaction, delta, 9L), Errors.None)
      certify(first, transaction)
    finally first.close()

    val recovered = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(recovered.snapshot.pending, 1)
      assertEquals(recovered.transactionStatus(transaction).status, CoordinatorTransactionStatus.Committed)
      assertEquals(recovered.finalizeTransaction(transaction).toOption.get.groupImage.offsets.head.value.offset, 82L)
    finally recovered.close()
  }

  test("multi-shard transactions recover atomically only after every finalize marker") {
    val directory = Files.createTempDirectory("cascade-coordinator-atomic")
    val (_, groupChange) = groupDelta("atomic", 123L, 11L)
    val allocator = CoordinatorShardUpdate(
      CoordinatorShard.Allocator,
      baseline.shardVersion(CoordinatorShard.Allocator),
      ByteWriter().writeLong(2L).result().toVector
    )
    val delta = groupChange.copy(updates = groupChange.updates :+ allocator)
    val transaction = CoordinatorTransactionId(3L, 3L)
    val store = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(store.prepare(transaction, delta, 11L), Errors.None)
      certify(store, transaction)
      val committed = store.finalizeTransaction(transaction).toOption.get
      assertEquals(committed.groupImage.offsets.head.value.offset, 123L)
      assertEquals(committed.deliveryImage.nextProducerId, 2L)
      assertEquals(Files.list(directory).count(), 2L)
    finally store.close()

    val recovered = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(recovered.metadata.groupImage.offsets.head.value.offset, 123L)
      assertEquals(recovered.metadata.deliveryImage.nextProducerId, 2L)
    finally recovered.close()
  }

  test("rejects overlap and releases a shard after an abort") {
    val directory = Files.createTempDirectory("cascade-coordinator-conflict")
    val (_, first) = groupDelta("conflict", 1L, 4L)
    val second = first.copy(updates = first.updates.map(_.copy(payload = first.updates.head.payload.updated(0, 1.toByte))))
    val firstId = CoordinatorTransactionId(4L, 1L)
    val secondId = CoordinatorTransactionId(4L, 2L)
    val store = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(store.prepare(firstId, first, 4L), Errors.None)
      assertEquals(store.prepare(secondId, second, 4L), Errors.CoordinatorLoadInProgress)
      store.abort(firstId)
      assertEquals(store.prepare(secondId, second, 4L), Errors.None)
      assertEquals(store.snapshot.aborted, 1L)
      assertEquals(store.snapshot.conflicts, 1L)
    finally store.close()
  }

  test("resumes a prepare interrupted between shard journal forces") {
    val directory = Files.createTempDirectory("cascade-coordinator-partial-prepare")
    val (_, groupChange) = groupDelta("partial", 211L, 13L)
    val allocator = CoordinatorShardUpdate(
      CoordinatorShard.Allocator,
      baseline.shardVersion(CoordinatorShard.Allocator),
      ByteWriter().writeLong(2L).result().toVector
    )
    val delta = groupChange.copy(updates = groupChange.updates :+ allocator)
    val transaction = CoordinatorTransactionId(5L, 5L)
    val firstShard = delta.updates.map(_.id).min
    val partial = CoordinatorShardJournal(CoordinatorShardJournal.path(directory, firstShard), firstShard)
    try partial.append(CoordinatorQuorumRecord.prepare(transaction, delta))
    finally partial.close()

    val recovered = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(recovered.snapshot.pending, 1)
      assertEquals(recovered.prepare(transaction, delta, 13L), Errors.None)
      certify(recovered, transaction)
      val committed = recovered.finalizeTransaction(transaction).toOption.get
      assertEquals(committed.groupImage.offsets.head.value.offset, 211L)
      assertEquals(committed.deliveryImage.nextProducerId, 2L)
      assertEquals(Files.list(directory).count(), 2L)
    finally recovered.close()
  }

  test("accepts monotonic metadata checkpoints and rejects crossing baselines") {
    val directory = Files.createTempDirectory("cascade-coordinator-baseline")
    val store = CoordinatorShardStore(directory, baseline)
    val versions = Vector.fill(CoordinatorShard.Count)(2L)
    try
      store.installBaseline(baseline.copy(version = 2L, shardVersions = versions))
      assertEquals(store.metadata.version, 2L)
      store.installBaseline(baseline)
      assertEquals(store.metadata.version, 2L)
      val crossing = versions.updated(0, 1L).updated(1, 3L)
      intercept[IllegalArgumentException](store.installBaseline(baseline.copy(version = 3L, shardVersions = crossing)))
    finally store.close()
  }

  test("does not finalize an uncertified decision vote") {
    val directory = Files.createTempDirectory("cascade-coordinator-uncertified")
    val (_, delta) = groupDelta("uncertified", 301L, 15L)
    val transaction = CoordinatorTransactionId(6L, 6L)
    val store = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(store.prepare(transaction, delta, 15L), Errors.None)
      assertEquals(store.decide(transaction), Errors.None)
      assertEquals(store.finalizeTransaction(transaction), Left(Errors.InvalidRequest))
      assertEquals(store.transactionStatus(transaction).status, CoordinatorTransactionStatus.Voted)
    finally store.close()
  }

  test("a certified recovery can fill a missing participant under a later controller term") {
    val directory = Files.createTempDirectory("cascade-coordinator-certified-recovery")
    val (_, delta) = groupDelta("recover", 401L, 17L)
    val transaction = CoordinatorTransactionId(7L, 7L)
    val store = CoordinatorShardStore(directory, baseline)
    try
      assertEquals(store.recoverCertified(transaction, delta, certificate), Errors.None)
      assertEquals(store.transactionStatus(transaction).status, CoordinatorTransactionStatus.Committed)
      assertEquals(store.finalizeTransaction(transaction).toOption.get.groupImage.offsets.head.value.offset, 401L)
    finally store.close()
  }

  private def groupDelta(seed: String, offset: Long, term: Long): (String, CoordinatorDelta) =
    val group = Iterator.from(0).map(index => s"$seed-$index").find(value => CoordinatorShard.group(value) != 0).get
    val shard = CoordinatorShard.group(group)
    val value = OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, 1L))
    val image = GroupCodec.encode(GroupImage(0L, Vector.empty, Vector(value))).toVector
    val payload = GroupShardCodec.split(image)(shard)
    group -> CoordinatorDelta(term, Vector(CoordinatorShardUpdate(shard, baseline.shardVersion(shard), payload)))

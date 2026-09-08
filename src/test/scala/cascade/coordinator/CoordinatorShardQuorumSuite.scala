package cascade.coordinator

import cascade.cluster.{ClusterNode, CoordinatorMetadata, QuorumMembership}
import cascade.group.{CommittedOffset, GroupCodec, GroupImage, GroupOffsetKey, GroupShardCodec, OffsetCommitValue}
import cascade.protocol.Errors
import java.nio.file.Files
import java.util.concurrent.{CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import munit.FunSuite

final class CoordinatorShardQuorumSuite extends FunSuite:
  private val nodes = Vector(
    ClusterNode(1, "node-1", 9092),
    ClusterNode(2, "node-2", 9092),
    ClusterNode(3, "node-3", 9092)
  )
  private val membership = QuorumMembership.bootstrap(nodes)

  test("commits prepare decision and finalize to a voter majority") {
    val fixture = QuorumFixture(nodes)
    try
      val delta = groupDelta("orders", 51L, 10L)
      val quorum = fixture.quorum()
      try
        assert(quorum.commit(delta))
        assertEquals(fixture.stores.map(_.metadata.groupImage.offsets.head.value.offset), Vector(51L, 51L, 51L))
        assertEquals(quorum.snapshot.committed, 1L)
        assertEquals(quorum.snapshot.prepareMessages, 3L)
        assertEquals(quorum.snapshot.decisionMessages, 3L)
        assertEquals(quorum.snapshot.finalizeMessages, 3L)
        assertEquals(quorum.snapshot.store.pending, 0)
      finally quorum.close()
    finally fixture.closeRemotes()
  }

  test("commits with one failed voter and aborts without a majority") {
    val oneFailure = QuorumFixture(nodes, failedNodes = Set(3))
    try
      val quorum = oneFailure.quorum()
      try assert(quorum.commit(groupDelta("available", 61L, 10L)))
      finally quorum.close()
    finally oneFailure.closeRemotes()

    val twoFailures = QuorumFixture(nodes, failedNodes = Set(2, 3))
    try
      val quorum = twoFailures.quorum()
      try
        assert(!quorum.commit(groupDelta("unavailable", 71L, 10L)))
        assertEquals(quorum.metadata.groupImage.offsets, Vector.empty)
        assertEquals(quorum.snapshot.failed, 1L)
        assertEquals(quorum.snapshot.store.aborted, 1L)
      finally quorum.close()
    finally twoFailures.closeRemotes()
  }

  test("disjoint shard commits enter replication concurrently") {
    val first = groupDelta("parallel-a", 81L, 10L)
    val second = Iterator.from(0).map(index => groupDelta(s"parallel-b-$index", 82L, 10L))
      .find(_.updates.head.id != first.updates.head.id).get
    val entered = CountDownLatch(2)
    val release = CountDownLatch(1)
    val fixture = QuorumFixture(nodes, prepareBarrier = Some(entered -> release))
    val quorum = fixture.quorum()
    val executor = Executors.newFixedThreadPool(2)
    try
      val a = executor.submit(() => quorum.commit(first))
      val b = executor.submit(() => quorum.commit(second))
      assert(entered.await(5L, TimeUnit.SECONDS), "disjoint commits did not overlap in replication")
      release.countDown()
      assert(a.get(5L, TimeUnit.SECONDS))
      assert(b.get(5L, TimeUnit.SECONDS))
      assertEquals(quorum.snapshot.committed, 2L)
      assert(quorum.snapshot.peakInflight >= 2)
    finally
      release.countDown()
      executor.shutdownNow(): Unit
      quorum.close()
      fixture.closeRemotes()
  }

  private def groupDelta(seed: String, offset: Long, term: Long): CoordinatorDelta =
    val group = s"$seed-group"
    val shard = CoordinatorShard.group(group)
    val value = OffsetCommitValue(GroupOffsetKey(group, "events", 0), CommittedOffset(offset, -1, None, 1L))
    val image = GroupCodec.encode(GroupImage(0L, Vector.empty, Vector(value))).toVector
    CoordinatorDelta(term, Vector(CoordinatorShardUpdate(shard, 0L, GroupShardCodec.split(image)(shard))))

  private final case class QuorumFixture(
      clusterNodes: Vector[ClusterNode],
      failedNodes: Set[Int] = Set.empty,
      prepareBarrier: Option[(CountDownLatch, CountDownLatch)] = None
  ):
    val stores: Vector[CoordinatorShardStore] = clusterNodes.map(node =>
      CoordinatorShardStore(Files.createTempDirectory(s"cascade-quorum-${node.id}"), CoordinatorMetadata.Empty)
    )
    private val installed = AtomicReference(CoordinatorMetadata.Empty)

    def quorum(): CoordinatorShardQuorum = CoordinatorShardQuorum(
      1,
      stores.head,
      CoordinatorQuorumConfig(maxInflightTransactions = 8, admissionTimeoutMillis = 1000L),
      () => membership,
      () => 10L,
      (targets, record) =>
        if record.phase == CoordinatorQuorumPhase.Prepare then
          prepareBarrier.foreach { case (entered, release) =>
            entered.countDown()
            release.await(5L, TimeUnit.SECONDS): Unit
          }
        targets.map { node =>
          val code = if failedNodes(node.id) then Errors.RequestTimedOut else apply(stores(node.id - 1), record)
          node.id -> code
        }.toMap,
      installed.set
    )

    def closeRemotes(): Unit = stores.drop(1).foreach(_.close())

    private def apply(store: CoordinatorShardStore, record: CoordinatorQuorumRecord): Short = record.phase match
      case CoordinatorQuorumPhase.Prepare => store.prepare(record.transactionId, record.delta.get, 10L)
      case CoordinatorQuorumPhase.Decide  => store.decide(record.transactionId)
      case CoordinatorQuorumPhase.Finalize => store.finalizeTransaction(record.transactionId).fold(identity, _ => Errors.None)
      case CoordinatorQuorumPhase.Abort =>
        store.abort(record.transactionId)
        Errors.None

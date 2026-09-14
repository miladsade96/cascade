package cascade.protocol

import munit.FunSuite

final class CompatibilityContractSuite extends FunSuite:
  test("the 1.0.0 Kafka API contract never narrows silently") {
    val release100 = Vector(
      ApiVersion(ApiKey.Produce, 3, 3),
      ApiVersion(ApiKey.Fetch, 6, 6),
      ApiVersion(ApiKey.ListOffsets, 2, 2),
      ApiVersion(ApiKey.Metadata, 4, 4),
      ApiVersion(ApiKey.OffsetCommit, 5, 7),
      ApiVersion(ApiKey.OffsetFetch, 4, 5),
      ApiVersion(ApiKey.FindCoordinator, 2, 2),
      ApiVersion(ApiKey.JoinGroup, 5, 5),
      ApiVersion(ApiKey.Heartbeat, 3, 3),
      ApiVersion(ApiKey.LeaveGroup, 2, 2),
      ApiVersion(ApiKey.SyncGroup, 3, 3),
      ApiVersion(ApiKey.DescribeGroups, 0, 4),
      ApiVersion(ApiKey.ListGroups, 0, 4),
      ApiVersion(ApiKey.SaslHandshake, 1, 1),
      ApiVersion(ApiKey.ApiVersions, 0, 4),
      ApiVersion(ApiKey.CreateTopics, 2, 2),
      ApiVersion(ApiKey.InitProducerId, 1, 1),
      ApiVersion(ApiKey.AddPartitionsToTxn, 1, 1),
      ApiVersion(ApiKey.AddOffsetsToTxn, 1, 1),
      ApiVersion(ApiKey.EndTxn, 1, 1),
      ApiVersion(ApiKey.TxnOffsetCommit, 2, 2),
      ApiVersion(ApiKey.DescribeAcls, 1, 1),
      ApiVersion(ApiKey.CreateAcls, 1, 1),
      ApiVersion(ApiKey.DeleteAcls, 1, 1),
      ApiVersion(ApiKey.DescribeConfigs, 2, 2),
      ApiVersion(ApiKey.DeleteGroups, 0, 1),
      ApiVersion(ApiKey.IncrementalAlterConfigs, 0, 0),
      ApiVersion(ApiKey.SaslAuthenticate, 1, 1),
      ApiVersion(ApiKey.AlterPartitionReassignments, 0, 0),
      ApiVersion(ApiKey.ListPartitionReassignments, 0, 0),
      ApiVersion(ApiKey.DescribeQuorum, 0, 2),
      ApiVersion(ApiKey.AddRaftVoter, 0, 1),
      ApiVersion(ApiKey.RemoveRaftVoter, 0, 0)
    )
    val current = Compatibility.supported.map(version => version.apiKey -> version).toMap
    release100.foreach { required =>
      val available = current.getOrElse(required.apiKey, fail(s"API ${required.apiKey} was removed"))
      assert(available.minVersion <= required.minVersion, s"API ${required.apiKey} minimum version increased")
      assert(available.maxVersion >= required.maxVersion, s"API ${required.apiKey} maximum version decreased")
    }
  }

  test("the 1.5.0 consumer protocol contract advertises every implemented version") {
    val current = Compatibility.supported.map(version => version.apiKey -> version).toMap
    assertEquals(current(ApiKey.ConsumerGroupHeartbeat), ApiVersion(ApiKey.ConsumerGroupHeartbeat, 0, 1))
    assertEquals(current(ApiKey.ConsumerGroupDescribe), ApiVersion(ApiKey.ConsumerGroupDescribe, 0, 1))
    assertEquals(current(ApiKey.OffsetCommit), ApiVersion(ApiKey.OffsetCommit, 5, 10))
    assertEquals(current(ApiKey.OffsetFetch), ApiVersion(ApiKey.OffsetFetch, 4, 10))
    assertEquals(current(ApiKey.OffsetDelete), ApiVersion(ApiKey.OffsetDelete, 0, 0))
    assertEquals(current(ApiKey.ListGroups), ApiVersion(ApiKey.ListGroups, 0, 5))
    assertEquals(current(ApiKey.DeleteGroups), ApiVersion(ApiKey.DeleteGroups, 0, 2))
    assertEquals(current(ApiKey.DescribeProducers), ApiVersion(ApiKey.DescribeProducers, 0, 0))
    assertEquals(current(ApiKey.DescribeTransactions), ApiVersion(ApiKey.DescribeTransactions, 0, 0))
    assertEquals(current(ApiKey.ListTransactions), ApiVersion(ApiKey.ListTransactions, 0, 2))
    assert(Compatibility.isFlexibleRequest(ApiKey.ConsumerGroupDescribe, 0))
    assert(Compatibility.isFlexibleRequest(ApiKey.OffsetCommit, 8))
    assert(Compatibility.isFlexibleRequest(ApiKey.OffsetFetch, 6))
    assert(!Compatibility.isFlexibleRequest(ApiKey.OffsetDelete, 0))
    assert(Compatibility.isFlexibleRequest(ApiKey.DescribeProducers, 0))
    assert(Compatibility.isFlexibleRequest(ApiKey.DescribeTransactions, 0))
    assert(Compatibility.isFlexibleRequest(ApiKey.ListTransactions, 2))
  }

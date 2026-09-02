package cascade.coordinator

import cascade.cluster.CoordinatorMetadata
import munit.FunSuite

final class CoordinatorVersionSuite extends FunSuite:
  test("legacy images seed every shard from the acknowledged global version") {
    val legacy = CoordinatorMetadata.Empty.copy(version = 17L)
    assert((0 until CoordinatorShard.Count).forall(legacy.shardVersion(_) == 17L))
    val sharded = legacy.copy(shardVersions = Vector.fill(CoordinatorShard.Count)(3L).updated(4, 5L))
    assertEquals(sharded.shardVersion(4), 5L)
    assertEquals(sharded.shardVersion(3), 3L)
  }

  test("invalid layouts and conflict versions are rejected") {
    intercept[IllegalArgumentException](CoordinatorMetadata.Empty.copy(shardVersions = Vector(1L)))
    intercept[IllegalArgumentException](CoordinatorMetadata.Empty.copy(shardVersions = Vector.fill(129)(-1L)))
    intercept[IllegalArgumentException](CoordinatorMetadata.Empty.shardVersion(129))
  }

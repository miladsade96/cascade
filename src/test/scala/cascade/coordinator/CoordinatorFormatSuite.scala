package cascade.coordinator

import cascade.cluster.*
import cascade.protocol.{ByteCursor, ProtocolException}
import munit.FunSuite

final class CoordinatorFormatSuite extends FunSuite:
  test("format nine persists shard versions and rejects lossy downgrade") {
    val state = CoordinatorMetadata.Empty.copy(shardVersions = Vector.tabulate(129)(_.toLong))
    val metadata = ClusterMetadata.Empty.copy(coordinator = state)
    val bytes = MetadataCodec.encode(metadata)
    assertEquals(ByteCursor(bytes).readShort(), 9.toShort)
    assertEquals(MetadataCodec.decode(bytes), metadata)
    intercept[ProtocolException](MetadataCodec.encode(metadata, 8))
  }

  test("feature activation raises the floor even before the first shard write") {
    val metadata = ClusterMetadata.Empty.copy(featureLevels = Map(ClusterFeature.CoordinatorDeltas -> 1.toShort))
    assertEquals(MetadataCodec.minimumRequiredFormat(metadata), 9.toShort)
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(ClusterMetadata.Empty, 8)).coordinator.shardVersions, Vector.empty)
    val old = PeerCapabilities.Current.copy(maxMetadataFormat = 8, featureLevels = Map.empty)
    assertEquals(NegotiatedCapabilities.across(Vector(old, PeerCapabilities.Current)).toOption.get.metadataFormat, 8.toShort)
  }

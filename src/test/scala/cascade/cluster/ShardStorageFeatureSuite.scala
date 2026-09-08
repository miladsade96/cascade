package cascade.cluster

import cascade.protocol.ProtocolException
import munit.FunSuite

final class ShardStorageFeatureSuite extends FunSuite:
  test("object references require format eleven and unanimous feature support") {
    val storageFeatures = PeerCapabilities.Current.featureLevels - ClusterFeature.IndependentCoordinator
    val active = MetadataDeltaFixture.base.copy(featureLevels = storageFeatures)
    assertEquals(MetadataCodec.minimumRequiredFormat(active), 11.toShort)
    assertEquals(MetadataCodec.decode(MetadataCodec.encode(active)), active)
    intercept[ProtocolException](MetadataCodec.encode(active, 10))
    val previous = PeerCapabilities.Current.copy(maxMetadataFormat = 10,
      featureLevels = storageFeatures - ClusterFeature.ShardObjectStorage)
    val mixed = NegotiatedCapabilities.across(Vector(previous, PeerCapabilities.Current)).toOption.get
    assertEquals(mixed.metadataFormat, 10.toShort)
    assertEquals(mixed.featureLevel(ClusterFeature.ShardObjectStorage), 0.toShort)
    assertEquals(MetadataCodec.minimumRequiredFormat(active.copy(featureLevels = previous.featureLevels)), 10.toShort)
  }

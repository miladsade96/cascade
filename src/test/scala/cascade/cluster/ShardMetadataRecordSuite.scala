package cascade.cluster

import cascade.protocol.ProtocolException
import munit.FunSuite

final class ShardMetadataRecordSuite extends FunSuite:
  import ShardStorageFixture.*

  test("self-contained checkpoints recover original image bytes and delta markers remain small") {
    withDirectory { root =>
      val objects = ShardObjectStore(root)
      val (_, next) = MetadataDeltaFixture.update(active)
      val full = ShardMetadataRecord.checkpoint(active, objects)
      assert(!full.isDelta)
      assertEquals(ShardMetadataRecord.replay(full.bytes, ClusterMetadata.Empty, objects).metadata, active)
      val delta = ShardMetadataRecord.prepare(active, next, objects)
      assert(delta.isDelta)
      assertEquals(delta.references.size, 1)
      assert(delta.bytes.length < 160)
      assertEquals(ShardMetadataRecord.replay(delta.bytes, active, objects).metadata, next)
      intercept[ProtocolException](ShardMetadataRecord.replay(delta.bytes, next, objects)): Unit
    }
  }

  test("migration and structural changes use checkpoints rather than incremental references") {
    withDirectory { root =>
      val objects = ShardObjectStore(root)
      val previous = active.copy(version = active.version - 1L,
        featureLevels = active.featureLevels - ClusterFeature.ShardObjectStorage)
      val migrated = ShardMetadataRecord.prepare(previous, active, objects)
      assert(!migrated.isDelta)
      val next = active.copy(version = active.version + 1, unavailableBrokerIds = Set(2))
      assert(!ShardMetadataRecord.prepare(active, next, objects).isDelta)
    }
  }

  test("truncated trailing and unknown marker kinds fail decoding") {
    withDirectory { root =>
      val objects = ShardObjectStore(root)
      val record = ShardMetadataRecord.checkpoint(active, objects).bytes
      for end <- 0 until record.length do intercept[Exception](ShardMetadataRecord.replay(record.take(end), active, objects))
      intercept[ProtocolException](ShardMetadataRecord.replay(record :+ 0.toByte, active, objects))
      val unknown = record.clone()
      unknown(2) = 2
      intercept[ProtocolException](ShardMetadataRecord.replay(unknown, active, objects)): Unit
    }
  }

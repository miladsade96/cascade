package cascade.cluster

import cascade.protocol.{ByteWriter, ProtocolException}
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

  test("aggregate reference lengths are bounded before any object is materialized") {
    withDirectory { root =>
      val objects = ShardObjectStore(root)
      val header = MetadataCodec.encode(active.copy(coordinator = active.coordinator.copy(
        groupState = Vector.empty, deliveryState = Vector.empty)))
      val full = ByteWriter().writeShort(ShardMetadataRecord.Format).writeByte(0).writeByteArray(header)
      ShardObjectRef.write(full, ShardObjectRef(129, ShardObjectRef.MaximumBytes, Vector.fill(32)(0.toByte)))
      ShardObjectRef.write(full, ShardObjectRef(130, 1, Vector.fill(32)(0.toByte)))
      val fullError = intercept[ProtocolException](ShardMetadataRecord.replay(full.result(), active, objects))
      assert(fullError.getMessage.contains("bounded recovery limit"))
      val delta = ByteWriter().writeShort(ShardMetadataRecord.Format).writeByte(1)
        .writeLong(active.version).writeLong(active.coordinator.version).writeBytes(active.fingerprint.toArray)
        .writeLong(active.controllerTerm).writeInt(2)
      Vector(0, 1).foreach { shard =>
        delta.writeLong(0L)
        ShardObjectRef.write(delta, ShardObjectRef(shard, ShardObjectRef.MaximumBytes, Vector.fill(32)(0.toByte)))
      }
      val deltaError = intercept[ProtocolException](ShardMetadataRecord.replay(delta.result(), active, objects))
      assert(deltaError.getMessage.contains("bounded recovery limit"))
      assertEquals(objects.snapshot.liveBytes, 0L)
    }
  }

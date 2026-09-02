package cascade.cluster

import cascade.protocol.{ByteWriter, ProtocolException}
import java.nio.file.{Files, Path, StandardOpenOption}
import java.util.zip.CRC32C
import munit.FunSuite

final class MetadataDeltaRecoverySuite extends FunSuite:
  import MetadataDeltaFixture.*

  private def withJournal(test: Path => Unit): Unit =
    val directory = Files.createTempDirectory("cascade-delta-recovery")
    val path = directory.resolve("metadata.log")
    try test(path)
    finally
      Files.deleteIfExists(path): Unit
      Files.deleteIfExists(path.resolveSibling("metadata.log.cleaned")): Unit
      Files.deleteIfExists(directory): Unit

  private def frame(payload: Array[Byte]): Array[Byte] =
    val crc = CRC32C()
    crc.update(payload, 0, payload.length)
    ByteWriter().writeInt(payload.length).writeBytes(payload).writeInt(crc.getValue.toInt).result()

  test("every torn delta boundary preserves the preceding forced image") {
    withJournal { path =>
      val baseline = frame(MetadataCodec.encode(base))
      val (delta, _) = update(base)
      val record = frame(MetadataDeltaCodec.encode(delta))
      (1 until record.length).foreach { length =>
        Files.write(path, baseline ++ record.take(length)): Unit
        val recovered = MetadataStore(path)
        try
          assertEquals(recovered.metadata, base)
          assertEquals(recovered.journalSize, baseline.length.toLong)
        finally recovered.close()
      }
      Files.write(path, baseline ++ record.updated(record.length - 1, (record.last ^ 1).toByte)): Unit
      val recovered = MetadataStore(path)
      try assertEquals(recovered.metadata, base)
      finally recovered.close()
    }
  }

  test("checksummed missing wrong-base and repeated deltas fail without truncating evidence") {
    withJournal { path =>
      val (delta, _) = update(base)
      val baseline = frame(MetadataCodec.encode(base))
      val record = frame(MetadataDeltaCodec.encode(delta))
      Vector(record, baseline ++ frame(MetadataDeltaCodec.encode(delta.copy(baseVersion = 99L))), baseline ++ record ++ record)
        .foreach { bytes =>
          Files.write(path, bytes): Unit
          intercept[ProtocolException](MetadataStore(path))
          assertEquals(Files.size(path), bytes.length.toLong)
        }
    }
  }

  test("atomic checkpoint replacement preserves exact state and accepts subsequent deltas") {
    withJournal { path =>
      var image = base
      val store = MetadataStore(path, compactionBytes = 1024L)
      try
        store.commit(image)
        (1 to 80).foreach { offset =>
          image = update(image, s"group-${offset % 20}", offset.toLong)._2
          store.commit(image)
        }
        assert(store.journalSize < MetadataCodec.encode(image).length.toLong + 2048L)
      finally store.close()
      val recovered = MetadataStore(path, compactionBytes = 1024L)
      try
        assertEquals(recovered.metadata, image)
        val next = update(image, "final", 90L)._2
        recovered.commit(next)
        image = next
      finally recovered.close()
      val finalStore = MetadataStore(path)
      try assertEquals(finalStore.metadata, image)
      finally finalStore.close()
    }
  }

  test("startup discards an unpublished checkpoint but finishes replacement when the target is absent") {
    withJournal { path =>
      val (_, next) = update(base)
      Files.write(path, frame(MetadataCodec.encode(base))): Unit
      Files.write(path.resolveSibling("metadata.log.cleaned"), frame(MetadataCodec.encode(next)), StandardOpenOption.CREATE_NEW): Unit
      val recovered = MetadataStore(path)
      try assertEquals(recovered.metadata, base)
      finally recovered.close()
      Files.delete(path)
      Files.write(path.resolveSibling("metadata.log.cleaned"), frame(MetadataCodec.encode(next)), StandardOpenOption.CREATE_NEW): Unit
      val completed = MetadataStore(path)
      try assertEquals(completed.metadata, next)
      finally completed.close()
    }
  }

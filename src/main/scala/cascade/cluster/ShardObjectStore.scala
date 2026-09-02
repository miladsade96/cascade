package cascade.cluster

import cascade.protocol.ProtocolException
import cascade.storage.AtomicFileLifecycle
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.{AccessDeniedException, Files, LinkOption, Path, StandardOpenOption}
import java.util.concurrent.atomic.AtomicLong
import scala.jdk.CollectionConverters.*

/** Immutable payload preparation. The caller serializes publication and reclamation. */
final class ShardObjectStore(val directory: Path):
  private val locks = Vector.fill(ShardObjectRef.DeliverySnapshot + 1)(Object())
  private val writtenBytes = AtomicLong()
  private val writtenObjects = AtomicLong()
  private val reusedObjects = AtomicLong()
  private val reclaimedBytes = AtomicLong()
  private val reclaimedObjects = AtomicLong()
  require(!Files.isSymbolicLink(directory), "shard object directory must not be a symbolic link")
  Files.createDirectories(directory)
  private val directoryForceSupported = ShardObjectStore.forceDirectory(directory.getParent)
  private val liveBytes = AtomicLong(objectPaths().map(Files.size).sum)

  def snapshot: ShardObjectSnapshot = ShardObjectSnapshot(writtenBytes.get(), writtenObjects.get(),
    reusedObjects.get(), reclaimedBytes.get(), reclaimedObjects.get(), liveBytes.get(), directoryForceSupported)

  def put(shard: Int, bytes: Array[Byte]): ShardObjectRef =
    require(bytes.length <= ShardObjectRef.MaximumBytes, "shard object exceeds size limit")
    val ref = ShardObjectRef.identify(shard, bytes)
    locks(shard).synchronized {
      val target = directory.resolve(ref.fileName)
      if Files.exists(target, LinkOption.NOFOLLOW_LINKS) then
        read(ref) // Never reuse a corrupt object, even when its filename looks correct.
        reusedObjects.incrementAndGet(): Unit
      else
        val temporary = Files.createTempFile(directory, "prepare-", ".pending")
        try
          val channel = FileChannel.open(temporary, StandardOpenOption.WRITE)
          try
            val buffer = ByteBuffer.wrap(bytes)
            while buffer.hasRemaining do
              if channel.write(buffer) <= 0 then throw ProtocolException("shard object made no write progress")
            channel.force(true)
          finally channel.close()
          AtomicFileLifecycle.replace(temporary, target)
          // Publish the directory entry before a journal marker may reference it.
          ShardObjectStore.forceDirectory(directory): Unit
          writtenBytes.addAndGet(bytes.length.toLong): Unit
          writtenObjects.incrementAndGet(): Unit
          liveBytes.addAndGet(bytes.length.toLong): Unit
        finally Files.deleteIfExists(temporary): Unit
      ref
    }

  def read(ref: ShardObjectRef): Array[Byte] =
    val path = directory.resolve(ref.fileName)
    if !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS) then
      throw ProtocolException(s"missing or unsafe shard object: ${ref.fileName}")
    val channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)
    try
      if channel.size() != ref.length.toLong then throw ProtocolException(s"shard object length mismatch: ${ref.fileName}")
      val buffer = ByteBuffer.allocate(ref.length)
      while buffer.hasRemaining do
        if channel.read(buffer) <= 0 then throw ProtocolException(s"incomplete shard object: ${ref.fileName}")
      val bytes = buffer.array()
      if ShardObjectRef.identify(ref.shard, bytes) != ref then
        throw ProtocolException(s"shard object checksum mismatch: ${ref.fileName}")
      bytes
    finally channel.close()

  /** Only after a self-contained journal checkpoint has been forced and published. */
  def retain(references: Set[ShardObjectRef]): Unit =
    references.foreach(read)
    val retained = references.map(_.fileName)
    objectPaths().filterNot(path => retained(path.getFileName.toString)).foreach { path =>
      val size = Files.size(path)
      if Files.deleteIfExists(path) then
        liveBytes.addAndGet(-size): Unit
        reclaimedBytes.addAndGet(size): Unit
        reclaimedObjects.incrementAndGet(): Unit
    }
    val paths = Files.list(directory)
    try paths.iterator().asScala.filter(path => path.getFileName.toString.matches("prepare-[0-9]+\\.pending"))
      .filter(path => Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)).foreach(path => Files.deleteIfExists(path): Unit)
    finally paths.close()
    ShardObjectStore.forceDirectory(directory): Unit

  private def objectPaths(): Vector[Path] =
    val paths = Files.list(directory)
    try paths.iterator().asScala.filter { path =>
      val name = path.getFileName.toString
      name.matches("[0-9]{1,3}-[0-9a-f]{64}\\.shard") &&
        name.takeWhile(_ != '-').toInt <= ShardObjectRef.DeliverySnapshot &&
        Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)
    }.toVector
    finally paths.close()

object ShardObjectStore:
  def pathFor(journal: Path): Path = journal.resolveSibling(journal.getFileName.toString + ".shards")

  /** Windows/JDK cannot open directories for fsync; physical durability remains a hardware gate. */
  private[cluster] def forceDirectory(path: Path): Boolean =
    if path != null then
      try
        val channel = FileChannel.open(path, StandardOpenOption.READ)
        try channel.force(true)
        finally channel.close()
        true
      catch
        case _: AccessDeniedException if System.getProperty("os.name").startsWith("Windows") => false
    else false

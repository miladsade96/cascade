package cascade.cluster

import cascade.coordinator.{CoordinatorDelta, CoordinatorShard, CoordinatorShardUpdate}
import cascade.protocol.{ByteCursor, ByteWriter, ProtocolException}

final case class PreparedShardRecord(bytes: Array[Byte], references: Set[ShardObjectRef], isDelta: Boolean)
final case class ReplayedShardRecord(metadata: ClusterMetadata, references: Set[ShardObjectRef])

/** A forced journal marker is the only publication point for prepared shard objects. */
object ShardMetadataRecord:
  val Format: Short = -11

  def enabled(metadata: ClusterMetadata): Boolean =
    metadata.featureLevels.getOrElse(ClusterFeature.ShardObjectStorage, 0.toShort) >= 1 && MetadataDelta.enabled(metadata)

  def isRecord(bytes: Array[Byte]): Boolean = ByteCursor(bytes).readShort() == Format

  def prepare(base: ClusterMetadata, next: ClusterMetadata, objects: ShardObjectStore): PreparedShardRecord =
    require(enabled(next), "shard storage feature is not active")
    // Migration is always self-contained, even when the inline predecessor supports deltas.
    val delta = if enabled(base) then MetadataDelta.between(base, next) else None
    delta match
      case None => checkpoint(next, objects)
      case Some(value) =>
        snapshotHeader(next) // Preserve the bounded full-image recovery/peer fallback contract.
        if value.change.updates.iterator.map(_.payload.size.toLong).sum > ShardObjectRef.MaximumBytes then
          throw ProtocolException("shard delta exceeds the bounded recovery limit")
        val references = value.change.updates.map(update => objects.put(update.id, update.payload.toArray))
        val writer = ByteWriter().writeShort(Format).writeByte(1)
          .writeLong(value.baseVersion).writeLong(value.baseCoordinatorVersion)
          .writeBytes(value.baseFingerprint.toArray).writeLong(value.change.controllerTerm)
        writer.writeArray(value.change.updates.zip(references)) { case (update, ref) =>
          writer.writeLong(update.expectedVersion)
          ShardObjectRef.write(writer, ref)
        }
        PreparedShardRecord(writer.result(), references.toSet, isDelta = true)

  def checkpoint(metadata: ClusterMetadata, objects: ShardObjectStore): PreparedShardRecord =
    require(enabled(metadata), "shard storage feature is not active")
    val header = snapshotHeader(metadata)
    val group = objects.put(ShardObjectRef.GroupSnapshot, metadata.coordinator.groupState.toArray)
    val delivery = objects.put(ShardObjectRef.DeliverySnapshot, metadata.coordinator.deliveryState.toArray)
    val writer = ByteWriter().writeShort(Format).writeByte(0).writeByteArray(header)
    ShardObjectRef.write(writer, group)
    ShardObjectRef.write(writer, delivery)
    PreparedShardRecord(writer.result(), Set(group, delivery), isDelta = false)

  private def snapshotHeader(metadata: ClusterMetadata): Array[Byte] =
    val skeleton = metadata.copy(coordinator = metadata.coordinator.copy(groupState = Vector.empty, deliveryState = Vector.empty))
    val header = MetadataCodec.encode(skeleton)
    if header.length.toLong + metadata.coordinator.groupState.size + metadata.coordinator.deliveryState.size > ShardObjectRef.MaximumBytes then
      throw ProtocolException("coordinator image exceeds the bounded recovery limit")
    header

  def replay(bytes: Array[Byte], base: ClusterMetadata, objects: ShardObjectStore): ReplayedShardRecord =
    if bytes.length > ShardObjectRef.MaximumBytes then throw ProtocolException("shard marker exceeds size limit")
    val cursor = ByteCursor(bytes)
    if cursor.readShort() != Format then throw ProtocolException("unsupported shard marker format")
    cursor.readByte() match
      case 0 =>
        val header = cursor.readByteArray()
        val skeleton = MetadataCodec.decode(header)
        val group = ShardObjectRef.read(cursor)
        val delivery = ShardObjectRef.read(cursor)
        cursor.ensureFullyRead()
        if !enabled(skeleton) || skeleton.coordinator.groupState.nonEmpty || skeleton.coordinator.deliveryState.nonEmpty ||
          group.shard != ShardObjectRef.GroupSnapshot || delivery.shard != ShardObjectRef.DeliverySnapshot then
          throw ProtocolException("invalid shard checkpoint descriptor")
        if header.length.toLong + group.length + delivery.length > ShardObjectRef.MaximumBytes then
          throw ProtocolException("shard checkpoint exceeds the bounded recovery limit")
        val metadata = skeleton.copy(coordinator = skeleton.coordinator.copy(
          groupState = objects.read(group).toVector, deliveryState = objects.read(delivery).toVector))
        ReplayedShardRecord(metadata, Set(group, delivery))
      case 1 =>
        val version = cursor.readLong()
        val coordinatorVersion = cursor.readLong()
        val fingerprint = cursor.readBytes(32).toVector
        val term = cursor.readLong()
        val updates = cursor.readArray((cursor.readLong(), ShardObjectRef.read(cursor)))
        cursor.ensureFullyRead()
        if !enabled(base) || updates.isEmpty || updates.size > CoordinatorShard.Count ||
          updates.exists((_, ref) => !CoordinatorShard.valid(ref.shard)) then
          throw ProtocolException("invalid shard delta descriptor")
        if updates.iterator.map(_._2.length.toLong).sum > ShardObjectRef.MaximumBytes then
          throw ProtocolException("shard delta exceeds the bounded recovery limit")
        val change = CoordinatorDelta(term, updates.map { (expected, ref) =>
          CoordinatorShardUpdate(ref.shard, expected, objects.read(ref).toVector)
        })
        val result = MetadataDelta(version, coordinatorVersion, fingerprint, change).applyTo(base)
          .fold(message => throw ProtocolException(s"invalid shard delta: $message"), identity)
        snapshotHeader(result)
        ReplayedShardRecord(result, updates.map(_._2).toSet)
      case _ => throw ProtocolException("unknown shard marker kind")

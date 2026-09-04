package cascade.delivery

import cascade.coordinator.CoordinatorShard
import cascade.protocol.{ByteCursor, ByteWriter}

object DeliveryShardCodec:
  def split(bytes: Vector[Byte]): Vector[Vector[Byte]] =
    val image = if bytes.isEmpty then DeliveryImage.Empty else DeliveryCodec.decode(bytes.toArray)
    split(image)

  private[cascade] def split(image: DeliveryImage): Vector[Vector[Byte]] =
    partition(image).map(value => DeliveryCodec.encode(value).toVector) :+
      ByteWriter().writeLong(image.nextProducerId).result().toVector

  private[cascade] def partition(image: DeliveryImage): Vector[DeliveryImage] =
    val producers = image.producers.groupBy(p => CoordinatorShard.producer(p.producerId, p.transactionalId) - CoordinatorShard.Buckets)
    val active = image.activeTransactions.groupBy(t => CoordinatorShard.transaction(t.transactionalId) - CoordinatorShard.Buckets)
    val completed = image.completedTransactions.groupBy(t => CoordinatorShard.transaction(t.transactionalId) - CoordinatorShard.Buckets)
    Vector.tabulate(CoordinatorShard.Buckets) { id =>
      DeliveryImage(
        0L, 1L,
        producers.getOrElse(id, Vector.empty).sortBy(_.producerId),
        active.getOrElse(id, Vector.empty).sortBy(_.transactionalId),
        completed.getOrElse(id, Vector.empty)
      )
    }

  def merge(shards: Vector[Vector[Byte]], version: Long): Vector[Byte] =
    require(shards.size == CoordinatorShard.Buckets + 1, "invalid delivery shard count")
    val cursor = ByteCursor(shards.last.toArray)
    val nextProducerId = cursor.readLong()
    cursor.ensureFullyRead()
    require(nextProducerId > 0L, "invalid producer allocation counter")
    val images = shards.init.zipWithIndex.map { case (bytes, index) =>
      val id = index + CoordinatorShard.Buckets
      val image = DeliveryCodec.decode(bytes.toArray)
      require(image.producers.forall(p => CoordinatorShard.producer(p.producerId, p.transactionalId) == id), "producer in wrong shard")
      require(image.activeTransactions.forall(t => CoordinatorShard.transaction(t.transactionalId) == id), "transaction in wrong shard")
      require(image.completedTransactions.forall(t => CoordinatorShard.transaction(t.transactionalId) == id), "outcome in wrong shard")
      image
    }
    val producers = images.flatMap(_.producers).sortBy(_.producerId)
    val active = images.flatMap(_.activeTransactions).sortBy(_.transactionalId)
    require(producers.map(_.producerId).distinct.size == producers.size, "duplicate producer ID")
    val transactionalIds = producers.flatMap(_.transactionalId)
    require(transactionalIds.distinct.size == transactionalIds.size, "duplicate transactional ID")
    require(producers.forall(p => p.producerId > 0L && p.producerId < nextProducerId), "producer outside allocated range")
    require(active.map(_.transactionalId).distinct.size == active.size, "duplicate active transaction")
    DeliveryCodec.encode(DeliveryImage(version, nextProducerId, producers, active, images.flatMap(_.completedTransactions))).toVector

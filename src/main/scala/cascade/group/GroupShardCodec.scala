package cascade.group

import cascade.coordinator.CoordinatorShard

/** Stable shard payloads exclude the unrelated global image version. */
object GroupShardCodec:
  def split(bytes: Vector[Byte]): Vector[Vector[Byte]] =
    val image = if bytes.isEmpty then GroupImage.Empty else GroupCodec.decode(bytes.toArray)
    split(image)

  private[cascade] def split(image: GroupImage): Vector[Vector[Byte]] =
    partition(image).map(value => GroupCodec.encode(value).toVector)

  private[cascade] def partition(image: GroupImage): Vector[GroupImage] =
    val groups = image.groups.groupBy(value => CoordinatorShard.group(value.groupId))
    val offsets = image.offsets.groupBy(value => CoordinatorShard.group(value.key.groupId))
    val consumers = image.consumerGroups.groupBy(value => CoordinatorShard.group(value.groupId))
    Vector.tabulate(CoordinatorShard.Buckets) { id =>
      GroupImage(
        0L,
        groups.getOrElse(id, Vector.empty).sortBy(_.groupId),
        offsets.getOrElse(id, Vector.empty).sortBy(value => (value.key.groupId, value.key.topic, value.key.partition)),
        consumers.getOrElse(id, Vector.empty).sortBy(_.groupId)
      )
    }

  def merge(shards: Vector[Vector[Byte]], version: Long): Vector[Byte] =
    require(shards.size == CoordinatorShard.Buckets, "invalid group shard count")
    val images = shards.zipWithIndex.map { case (bytes, id) =>
      val image = GroupCodec.decode(bytes.toArray)
      require(image.groups.forall(value => CoordinatorShard.group(value.groupId) == id), "group in wrong shard")
      require(image.consumerGroups.forall(value => CoordinatorShard.group(value.groupId) == id), "consumer group in wrong shard")
      require(image.offsets.forall(value => CoordinatorShard.group(value.key.groupId) == id), "offset in wrong shard")
      require(image.groups.map(_.groupId).distinct.size == image.groups.size, "duplicate group")
      require(image.consumerGroups.map(_.groupId).distinct.size == image.consumerGroups.size, "duplicate consumer group")
      require(image.offsets.map(_.key).distinct.size == image.offsets.size, "duplicate offset")
      image
    }
    GroupCodec.encode(GroupImage(
      version,
      images.flatMap(_.groups).sortBy(_.groupId),
      images.flatMap(_.offsets).sortBy(value => (value.key.groupId, value.key.topic, value.key.partition)),
      images.flatMap(_.consumerGroups).sortBy(_.groupId)
    )).toVector

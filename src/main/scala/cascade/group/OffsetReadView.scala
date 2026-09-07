package cascade.group

/** Immutable, atomically replaceable view containing only acknowledged offsets. */
private[cascade] final case class OffsetReadView private (
    byKey: Map[GroupOffsetKey, CommittedOffset],
    byGroup: Map[String, Vector[(GroupOffsetKey, CommittedOffset)]]
):
  lazy val entries: Vector[OffsetCommitValue] = byKey.iterator
    .map { case (key, value) => OffsetCommitValue(key, value) }
    .toVector
    .sortBy(value => (value.key.groupId, value.key.topic, value.key.partition))

  def get(key: GroupOffsetKey): Option[CommittedOffset] = byKey.get(key)

  def all(groupId: String): Vector[(GroupOffsetKey, CommittedOffset)] =
    byGroup.getOrElse(groupId, Vector.empty)

  def updated(
      values: IterableOnce[OffsetCommitValue],
      removals: IterableOnce[GroupOffsetKey] = Iterable.empty
  ): OffsetReadView =
    val updates = values.iterator.map(value => value.key -> value.value).toMap
    val removed = removals.iterator.toSet -- updates.keySet
    if updates.isEmpty && removed.isEmpty then this
    else
      val touchedGroups = updates.keysIterator.map(_.groupId).toSet ++ removed.iterator.map(_.groupId)
      var nextGroups = byGroup
      touchedGroups.foreach { groupId =>
        val existing = byGroup.getOrElse(groupId, Vector.empty).toMap
        val groupRemoved = removed.filter(_.groupId == groupId)
        val groupUpdates = updates.iterator.filter(_._1.groupId == groupId).toMap
        val next = (existing -- groupRemoved) ++ groupUpdates
        if next.isEmpty then nextGroups -= groupId
        else nextGroups = nextGroups.updated(groupId, next.toVector.sortBy { case (key, _) => (key.topic, key.partition) })
      }
      OffsetReadView((byKey -- removed) ++ updates, nextGroups)

private[cascade] object OffsetReadView:
  val Empty: OffsetReadView = OffsetReadView(Map.empty, Map.empty)

  def from(values: IterableOnce[OffsetCommitValue]): OffsetReadView =
    val byKey = values.iterator.map(value => value.key -> value.value).toMap
    val byGroup = byKey.toVector.groupMap(_._1.groupId)(identity)
      .view.mapValues(_.sortBy { case (key, _) => (key.topic, key.partition) }).toMap
    OffsetReadView(byKey, byGroup)

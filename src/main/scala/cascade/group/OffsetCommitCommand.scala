package cascade.group

/** Immutable ordinary (non-transactional) commit. Transactional offsets use their existing atomic checkpoint. */
final case class OffsetCommitCommand(
    groupId: String,
    generationId: Int,
    memberId: String,
    groupInstanceId: Option[String],
    values: Vector[OffsetCommitValue]
):
  /** Conservative retained-object accounting, not wire bytes or an exact heap measurement. */
  def retainedBytes: Long =
    256L + 2L * (groupId.length.toLong + memberId.length + groupInstanceId.fold(0)(_.length)) +
      values.iterator.map { entry =>
        256L + 2L * (entry.key.groupId.length.toLong + entry.key.topic.length + entry.value.metadata.fold(0)(_.length))
      }.sum

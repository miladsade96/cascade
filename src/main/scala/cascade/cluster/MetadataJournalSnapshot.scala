package cascade.cluster

/** Process-lifetime successful forced writes; byte counters include length and CRC framing. */
final case class MetadataJournalSnapshot(
    fullRecords: Long = 0L,
    deltaRecords: Long = 0L,
    fullBytes: Long = 0L,
    deltaBytes: Long = 0L,
    checkpointBytes: Long = 0L,
    journalBytes: Long = 0L
)

object MetadataJournalSnapshot:
  val Empty: MetadataJournalSnapshot = MetadataJournalSnapshot()

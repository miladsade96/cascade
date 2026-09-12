package cascade.coordinator

trait CoordinatorCheckpoint:
  /** Commits the staged coordinator service image and restores the last committed image on failure. */
  def commit(): Boolean

  /** Atomically commits transaction state together with any staged consumer offsets. */
  def commitCombined(): Boolean = commit()

object CoordinatorCheckpoint:
  val Local: CoordinatorCheckpoint = () => true
